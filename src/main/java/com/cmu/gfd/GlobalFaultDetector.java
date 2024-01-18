package com.cmu.gfd;

import com.cmu.properties.Config;
import com.cmu.util.Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Optional;
import java.io.PrintWriter;

/**
 * The GlobalFaultDetector (GFD) class is responsible for maintaining a global
 * view of the system. It listens to heartbeats from LocalFaultDetectors (LFD)
 * and keeps track of active server replicas.
 *
 * @author Temitope Oguntade
 */
public class GlobalFaultDetector {

    static int member_count = 0;
    private final Set<String> membership = new HashSet<>();
    private final ConcurrentHashMap<String, Socket> lfds = new ConcurrentHashMap<>();
    private final Map<String, Long> lastHeartbeatReceived = new ConcurrentHashMap<>();
    private final Map<String, LinkedList<Long>> lfdHeartbeatHistory = new ConcurrentHashMap<>();
    private final Map<Socket, String> socketToLFDIDMap = new ConcurrentHashMap<>();
    private static final int HEARTBEAT_HISTORY_SIZE = 10;
    private PrintWriter rmWriter; // Writer to communicate with RM
    private Socket rmSocket;

    public GlobalFaultDetector() throws IOException {
        // Establish connection with RM
        try {
            this.rmSocket = new Socket(Config.RM_ADDRESS, Config.RM_LISTEN_PORT);
            this.rmWriter = new PrintWriter(rmSocket.getOutputStream(), true);
            Utilities.printInfo("Connected to RM at " + Config.RM_ADDRESS + ":" + Config.RM_LISTEN_PORT);
            sendUpdateToRM();
        } catch (IOException e) {
            Utilities.printError("Could not connect to RM: " + e.getMessage());
            // Exit or retry as needed.
            throw e;
        }
    }

    public void runServer() {
        Utilities.printState("GFD: " + member_count + " members");
        try (ServerSocket serverSocket = new ServerSocket(Config.GFD_LISTEN_PORT)) {
            Utilities.printInfo("GFD listening on port " + Config.GFD_LISTEN_PORT);
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (Exception e) {
            Utilities.printError("GFD Error: " + e.getMessage());
        }
    }

    private void handleClient(Socket clientSocket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.contains("_HEARTBEAT")) {
                    handleHeartbeat(inputLine, clientSocket);
                } else if (inputLine.contains(": add replica ") || inputLine.contains(": delete replica ")) {
                    handleLFD(inputLine, clientSocket);
                } else if (inputLine.contains("TERMINATE")) {
                    handleTermination(inputLine, clientSocket);
                } else {
                    Utilities.printWarning("Unknown message: " + inputLine);
                }

                Optional<String> lfdIdOpt = extractLFDIDFromMessage(inputLine);
                if (lfdIdOpt.isPresent()) {
                    String lfdId = lfdIdOpt.get();
                    Long lastReceived = lastHeartbeatReceived.get(lfdId);
                    if (lastReceived != null && isHeartbeatMissed(lfdId, lastReceived)) {
                        handleDisconnection(lfdId);
                        break;
                    }
                }
            }
            // Checking if the socket has been closed.
            if (clientSocket.isClosed()) {
                String lfdId = extractLFDIDFromSocket(clientSocket);
                if (lfdId != null) {
                    handleDisconnection(lfdId);
                }
            }
        } catch (IOException e) {
            Utilities.printError("Error handling LFD: " + e.getMessage());
        }
    }

    private void handleHeartbeat(String inputLine, Socket clientSocket) {
        String lfdId = inputLine.replace("_HEARTBEAT", "");
        socketToLFDIDMap.put(clientSocket, lfdId);
        long currentTime = System.currentTimeMillis();
        lastHeartbeatReceived.put(lfdId, currentTime);
        lfdHeartbeatHistory.computeIfAbsent(lfdId, k -> new LinkedList<>()).addLast(currentTime);
        if (lfdHeartbeatHistory.get(lfdId).size() > HEARTBEAT_HISTORY_SIZE) {
            lfdHeartbeatHistory.get(lfdId).removeFirst();
        }
        Utilities.printReceive("Received HEARTBEAT from " + lfdId);
    }

    private void handleLFD(String inputLine, Socket clientSocket) {
        String[] parts = inputLine.split(": ");
        String lfdId = parts[0];
        socketToLFDIDMap.put(clientSocket, lfdId);
        String action = parts[1].split(" ")[0];
        String serverId = parts[1].split(" ")[2];
        lastHeartbeatReceived.put(lfdId, System.currentTimeMillis());
        if ("add".equalsIgnoreCase(action)) {
            membership.add(serverId);
        } else if ("delete".equalsIgnoreCase(action)) {
            membership.remove(serverId);
        }
        Utilities.printInfo(inputLine);
        member_count = membership.size();
        Utilities.printState("GFD: " + member_count + " members: " + String.join(", ", membership));
        // After handling the add or delete actions, communicate the change to RM
        // Make sure that the update happens after the member count has been updated
        if ("add".equalsIgnoreCase(action) || "delete".equalsIgnoreCase(action)) {
            sendUpdateToRM();
        }
    }

    private void sendUpdateToRM() {
        String membershipList = membership.toString(); // This will be in the format "[S1, S2]"
        String updateMessage = "membership " + membershipList + " member_count " + member_count;
        rmWriter.println(updateMessage);
        Utilities.printInfo("Updated RM with: " + updateMessage);
    }

    private Optional<String> extractLFDIDFromMessage(String message) {
        if (message != null && message.contains(":")) {
            return Optional.of(message.split(":")[0]);
        }
        return Optional.empty();
    }

    private String extractLFDIDFromSocket(Socket clientSocket) {
        return socketToLFDIDMap.get(clientSocket);
    }

    private void handleDisconnection(String lfdId) {
        if (lfdId != null) {
            String replicaId = "S" + lfdId.substring(3);
            membership.remove(replicaId);
            member_count = membership.size();
            Utilities.printState("GFD: " + member_count + " members: " + String.join(", ", membership));
            Utilities.printInfo(replicaId + " considered dead as connection with " + lfdId + " broke.");
        }
    }

    private void handleTermination(String inputLine, Socket clientSocket) {
        Optional<String> lfdIdOpt = extractLFDIDFromMessage(inputLine);
        if (lfdIdOpt.isPresent()) {
            String lfdId = lfdIdOpt.get();
            handleDisconnection(lfdId);
        }
    }

    private boolean isHeartbeatMissed(String lfdId, long lastReceived) {
        LinkedList<Long> heartbeats = lfdHeartbeatHistory.get(lfdId);
        if (heartbeats.size() <= 1) {
            return false;
        }
        long totalInterval = 0;
        for (int i = 1; i < heartbeats.size(); i++) {
            totalInterval += (heartbeats.get(i) - heartbeats.get(i - 1));
        }
        long avgInterval = totalInterval / (heartbeats.size() - 1);
        return (System.currentTimeMillis() - lastReceived > 1.3 * avgInterval);
    }

    public static void main(String[] args) throws IOException {
        GlobalFaultDetector gfd = new GlobalFaultDetector();
        gfd.runServer();
    }
}
