package com.cmu.serverreplica;

import com.cmu.properties.Config;
import com.cmu.util.Utilities;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server class for receiving heartbeats and handling clients.
 *
 * @author Temitope Oguntade
 */
public class Server {

    private String LFD_ID = "";
    private static int my_state = 50;
    private static String SERVER_ID;
    private static String REPLICA_MODE;
    private static int CHECKPOINT_FREQ;
    private static int CHECKPOINT_COUNT = 1;
    private static final ConcurrentHashMap<String, Socket> clients = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> clientIds = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> otherServersIP = new ConcurrentHashMap<>(); // To store server ID against its IP
    private static final ConcurrentHashMap<String, Boolean> otherServersConnected = new ConcurrentHashMap<>(); // IP to connected status
    private static String currentPrimary = "";
    private static boolean isReady = false;
    private static String myIp = "";

    private static void connectToOtherServer(String otherServerIP) {
        new Thread(() -> {
            try (Socket socket = new Socket()) {
                while(true){
                    socket.connect(new InetSocketAddress(otherServerIP, Config.SERVER_LISTEN_PORT), 3000);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    if(currentPrimary.equals(SERVER_ID)){
                        out.println("PRIMARY " + currentPrimary);
                    }
                    out.println("INITIALCONNECT " + SERVER_ID);
                    String response = in.readLine();
                    if (response != null && response.startsWith("REGISTERED ")) {
                        String serverId = response.split(" ")[1];
                        otherServersIP.put(serverId, otherServerIP);
                        otherServersConnected.put(otherServerIP, true);
                        Utilities.printInfo("Connected to server: " + serverId);
                        // if (SERVER_ID.equalsIgnoreCase("s1")) {
                        //     String message = "CHECKPOINT " + my_state + " " + CHECKPOINT_COUNT;
                        //     System.out.println("======Message: " + message);
                        //     System.out.println("======otherServerIp: " + otherServerIP);
                        //     sendMessageToServer(otherServerIP, message);
                        //     Utilities.printState(SERVER_ID + " > " + serverId + " | Checkpoint_freq " + CHECKPOINT_FREQ + " | Checkpoint_count " + CHECKPOINT_COUNT);
                        //     System.out.println("========= inside connectToOtherServer line 55");
                        // }
                        String response2 = in.readLine();

                        break;
                    }
                    Thread.sleep(5000);
                }

            } catch (IOException e) {
                otherServersConnected.put(otherServerIP, false);
                Utilities.printError("Failed to connect to server at " + otherServerIP + ". Retrying...");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Utilities.printError("Connection attempt interrupted. Stopping retry.");
            }
            System.err.println("Finished connecting to server at " + otherServerIP);
        }).start();
    }

    private static void keepCheckServers(String serverIP) {
        new Thread(() -> {
            while (!Thread.interrupted()) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(serverIP, Config.SERVER_LISTEN_PORT), 3000);
                    socket.setSoTimeout(3000);

                    try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        if(currentPrimary.equals(SERVER_ID)){
                            out.println("PRIMARY " + currentPrimary);
                        }
                        out.println("CONNECT " + SERVER_ID);
                        String response = in.readLine();
                        if (response != null && response.startsWith("REGISTERED ")) {
                            String serverId = response.split(" ")[1];
                            otherServersIP.put(serverId, serverIP);
                            otherServersConnected.put(serverIP, true);
                            Utilities.printInfo("Keep checking: Connected to server: " + serverId);
                        }
                    }
                } catch (IOException e) {
                    otherServersConnected.put(serverIP, false);
                    Utilities.printError("Failed when keeping checking connection to server at " + serverIP + ". Retrying...");
                }

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Utilities.printError("Connection attempt interrupted. Stopping retry.");
                    break;
                }
            }
        }).start();
    }

    private static boolean isServerConnected(String backupIP) {
        return otherServersConnected.getOrDefault(backupIP, false);
    }

    public static void sendCheckpointData() {
        while (true) {
            if (currentPrimary.equals(SERVER_ID)) {
                try {
                    Thread.sleep(CHECKPOINT_FREQ);
                    for (Map.Entry<String, String> entry : otherServersIP.entrySet()) {
                        String serverId = entry.getKey();
                        String serverIP = entry.getValue();

                        // Check if the server is connected before sending
                        if (isServerConnected(serverIP)) {
                            sendMessageToServer(serverIP, "CHECKPOINT " + my_state + " " + CHECKPOINT_COUNT);
                            Utilities.printState(SERVER_ID + " > " + serverId + " | Checkpoint_freq " + CHECKPOINT_FREQ + " | Checkpoint_count " + CHECKPOINT_COUNT);
                        } else {
                            // If not connected, log a warning
                            Utilities.printWarning("Server " + serverId + " is not connected.");
                        }
                    }
                    CHECKPOINT_COUNT++;
                } catch (InterruptedException e) {
                    Utilities.printError("Error in checkpoint scheduler: " + e.getMessage());
                    // Handle interruption (e.g., consider breaking out of the loop)
                } catch (IOException ex) {
                    Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else {
                try {
                    // Sleep for a specified period before checking again
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    Utilities.printError("Send checkpoint data thread interrupted: " + e.getMessage());
                    break;
                }
            }
        }
    }


    private static void sendMessageToServer(String serverIP, String message) throws IOException {
        try (Socket socket = new Socket(serverIP, Config.SERVER_LISTEN_PORT);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(message);
        } catch (IOException e) {
            Utilities.printError("Failed to send message to server at " + serverIP + ": " + e.getMessage());
        }
    }

    public void runServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(Config.SERVER_LISTEN_PORT)) {
                Utilities.printInfo("Server " + SERVER_ID + " listening on port " + Config.SERVER_LISTEN_PORT);

                while (true) {
                    try {
                        serverSocket.setSoTimeout(5000); // Set a timeout of 5000 milliseconds
                        final Socket clientSocket = serverSocket.accept(); // This call will block for at most 5000 ms
                        new Thread(() -> handleClient(clientSocket)).start();
                    }catch (SocketTimeoutException e) {
                        System.out.println("No client connection received, checking server status...");
                        System.out.println("replica " + REPLICA_MODE);
                        System.err.println("currentPrimary: " + currentPrimary);
                        if (REPLICA_MODE.equals("passive") && !currentPrimary.equals("")) { // Check whether the current primary is active and re-elect if not (only in passive mode)
                            String currentPrimaryIp = "";
                            if (currentPrimary.equals(SERVER_ID)) {
                                currentPrimaryIp = myIp;
                            }else {
                                currentPrimaryIp = otherServersIP.get(currentPrimary);
                            }
                            System.err.println("currentPrimaryIp: " + currentPrimaryIp);
                            if (!currentPrimary.equals(SERVER_ID)) {
                                if (!otherServersConnected.get(currentPrimaryIp)) {
                                    electNewPrimary();
                                }
                            }
                        }
                    }catch (IOException e) {
                        // Handle other IOExceptions that may occur
                        Utilities.printError("IO Exception in server accept: " + e.getMessage());
                        break; // Optional: break the loop if server socket encounters an unrecoverable error
                    }
                }
            } catch (Throwable e) {
                e.printStackTrace();
                Utilities.printError("Server error: " + e.getMessage());
            }
        }).start();
    }

    public static boolean isAnyServerConnected() {
        for (Boolean connected : otherServersConnected.values()) {
            if (Boolean.TRUE.equals(connected)) {
                return true;
            }
        }
        return false;
    }

    private static void checkPrimaryStatus() {
        if (!isAnyServerConnected()) {
            electNewPrimary();
        }
    }

    private static void updateStateInOtherServers() {
        for (String serverIP : otherServersIP.values()) {
            if (otherServersConnected.getOrDefault(serverIP, false)) {
                try (Socket socket = new Socket(serverIP, Config.SERVER_LISTEN_PORT);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    out.println("UPDATE_STATE " + my_state);
                } catch (IOException e) {
                    Utilities.printError("Failed to update state on server at " + serverIP + ": " + e.getMessage());
                }
            }
        }
    }

    private static void electNewPrimary() {
        List<String> connectedServerIds = new ArrayList<>();
        for (Map.Entry<String, String> entry : otherServersIP.entrySet()) {
            if (otherServersConnected.getOrDefault(entry.getValue(), false)) {
                connectedServerIds.add(entry.getKey());

            }
        }
        connectedServerIds.add(SERVER_ID); // Add current server's ID
        Collections.sort(connectedServerIds);
        currentPrimary = connectedServerIds.get(0);

        if (currentPrimary.equals(SERVER_ID)) {
            isReady = true;
            Utilities.printState(SERVER_ID + " is now the primary server.");
        } else {
            isReady = "passive".equals(REPLICA_MODE); // Passive servers are always ready
            Utilities.printState(SERVER_ID + " is not the primary server. Current primary is " + currentPrimary);
        }

        // Notify all connected servers about the new primary
        // notifyOtherServersAboutNewPrimary();
    }

    private static void notifyOtherServersAboutNewPrimary() {
        for (String serverIP : otherServersIP.values()) {
            try (Socket socket = new Socket(serverIP, Config.SERVER_LISTEN_PORT);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println("NEW_PRIMARY " + currentPrimary);
            } catch (IOException e) {
                Utilities.printError("Failed to notify server at " + serverIP + " about new primary: " + e.getMessage());
            }
        }
    }

    private void closeConnection(Socket clientSocket) {
        try {
            if (clientSocket != null) {
                clientSocket.close();
            }
        } catch (IOException e) {
            Utilities.printWarning("Failed to close client socket.");
        }
    }

    private void handleClient(Socket clientSocket) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            String inputLine;
            // String inputLine = in.readLine();
            while ((inputLine = in.readLine()) != null) {

                switch (inputLine.split(" ")[0]) {
                    case "HEARTBEAT":
                        handleHeartbeat(clientSocket, out, inputLine);
                        break;
                    case "INIT_LFD":
                        handleInitLFD(clientSocket, out, in);
                        break;
                    case "CHECKPOINT":
                        handleCheckpoint(inputLine);
                        break;
                    case "INITIALCONNECT":
                        out.println("REGISTERED " + SERVER_ID);
                        if (SERVER_ID.equalsIgnoreCase("s1")) {
                            String message = "CHECKPOINT " + my_state + " " + CHECKPOINT_COUNT;
                            String ip = clientSocket.getRemoteSocketAddress().toString();
                            String otherServerId = inputLine.split(" ")[1];
                            ip = ip.replaceAll("/", "").split(":")[0];
                            sendMessageToServer(ip, message);
                            Utilities.printState(SERVER_ID + " > " + otherServerId + " | Checkpoint_freq " + CHECKPOINT_FREQ + " | Checkpoint_count " + CHECKPOINT_COUNT);
                        }
                        break;
                    case "CONNECT":
                        out.println("REGISTERED " + SERVER_ID);
                        break;
                    case "PRIMARY":
                        currentPrimary = inputLine.split(" ")[1];
                        break;
                    default:
                        handleRegularClient(clientSocket, out, in, inputLine);
                        break;
                }
            }
        } catch (IOException e) {
            Utilities.printError("Error handling client: " + e.getMessage());
            closeConnection(clientSocket);
        }
    }

    private void handleHeartbeat(Socket clientSocket, PrintWriter out, String inputLine) {
        int heartbeatCount = Integer.parseInt(inputLine.split(" ")[1]);
        Utilities.printHeartbeat("Received HEARTBEAT " + heartbeatCount + " from " + LFD_ID);
        out.println("ALIVE " + heartbeatCount);
        out.flush();
        Utilities.printHeartbeat("Sent ALIVE " + heartbeatCount + " to " + LFD_ID);
        // Connection is kept open for future heartbeats
    }

    private void handleInitLFD(Socket clientSocket, PrintWriter out, BufferedReader in) throws IOException {
        out.println(SERVER_ID); // Send server ID first
        out.flush();
        LFD_ID = in.readLine(); // Then receive LFD's ID
        // Handle LFD-specific logic
        handleLFD(clientSocket, in);
    }

    private void handleCheckpoint(String inputLine) {
        String[] parts = inputLine.split(" ");
        int receivedState = Integer.parseInt(parts[1]);
        int receivedCheckpointCount = Integer.parseInt(parts[2]);
        my_state = receivedState; // Update internal state
        CHECKPOINT_COUNT = receivedCheckpointCount; // Update checkpoint count

        isReady = true; // the server is ready when checkpoint received

        if ("active".equalsIgnoreCase(REPLICA_MODE)) {
            Utilities.printState("CHECKPOINT from " + "S1" + " | State: " + my_state + " | Checkpoint Count: " + CHECKPOINT_COUNT);
        }else {
            Utilities.printState("CHECKPOINT from " + currentPrimary + " | State: " + my_state + " | Checkpoint Count: " + CHECKPOINT_COUNT);
        }
    }

    private void handleLFD(Socket clientSocket, BufferedReader in) {
        String lfdId = LFD_ID;
        clients.put(lfdId, clientSocket);

        try {
            PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true);
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                if (inputLine.startsWith("HEARTBEAT ")) {
                    Utilities.printHeartbeat("Received from " + lfdId);
                    out.println("ALIVE");
                    out.flush();
                    Utilities.printHeartbeat("HEARTBEAT Sent to " + lfdId + ": ALIVE");
                }
            }
        } catch (IOException e) {
            clients.remove(lfdId);
            Utilities.printError("Error handling " + lfdId + ": " + e.getMessage());
        }
    }

    private void handleRegularClient(Socket clientSocket, PrintWriter out, BufferedReader in, String inputLine) {
        if (!isReady) {
            Utilities.printWarning("Server " + SERVER_ID + " is not ready to handle requests.");
            return;
        }

        // if ("active".equals(REPLICA_MODE) && currentPrimary.equals(SERVER_ID)) {
        //     // Update state in other servers before responding
        //     updateStateInOtherServers();
        // }

        // Store client information if not already done
        String clientId = inputLine; // Assuming the first message from client is its ID
        if (!clientIds.containsKey(clientId)) {
            clientIds.put(SERVER_ID, clientSocket.getRemoteSocketAddress().toString());
        }

        if (inputLine.charAt(0) == 'C') {
            out.println(SERVER_ID);
        }

        try {
            // Process the inputLine received as the first client message
            boolean continueHandling = processClientMessage(clientId, out, inputLine);
            // Continue to process further messages from the client if not disconnected
            while (continueHandling && (inputLine = in.readLine()) != null) {
                continueHandling = processClientMessage(clientId, out, inputLine);
            }
        } catch (IOException e) {
            Utilities.printError("Error handling client " + clientId + ": " + e.getMessage());
        } finally {
            clients.remove(clientId);
            closeConnection(clientSocket);
        }
    }

    private boolean processClientMessage(String clientId, PrintWriter out, String inputLine) {
        Utilities.printReceive("<" + clientId + ", " + SERVER_ID + ", " + inputLine + " request>");

        if ("QUIT".equalsIgnoreCase(inputLine)) {
            Utilities.printInfo(clientId + " disconnected.");
            return false;  // Stop processing further messages
        }

        String outputLine = String.valueOf(my_state);
        if (REPLICA_MODE.equals("active") || (REPLICA_MODE.equals("passive") && currentPrimary.equals(SERVER_ID))){
            out.println(outputLine);
            Utilities.printSend("<" + clientId + ", " + SERVER_ID + ", " + outputLine + " reply>");
            // if ("active".equals(REPLICA_MODE)) {
                modifyState(inputLine);
            //}
        } else {
            // For passive mode or non-primary servers in active mode
            if (!inputLine.startsWith("C")){
                Utilities.printLog("<" + clientId + ", " + SERVER_ID + ", " + inputLine + " received>");
            }
            out.println("IGNORE");
        }

        return true; // Continue processing further messages
    }

    private void modifyState(String message) {
        my_state++;
    }

    public static void main(String[] args) {
        REPLICA_MODE = args[1].toLowerCase();
        if (args.length < 6) {
            Utilities.printError("Please provide SERVER_ID, MODE (active/passive), CHECKPOINT_FREQ, SERVER_2_IP, and SERVER_3_IP. Exiting.");
            return;
        }
        if (args.length < 6 && REPLICA_MODE.equals("passive")) {
            Utilities.printError("Please provide SERVER_ID, MODE (active/passive), CHECKPOINT_FREQ, SERVER_2_IP, and SERVER_3_IP, PrimaryServerIs. Exiting.");
            return;
        }

        SERVER_ID = args[0];
        //REPLICA_MODE = args[1].toLowerCase();
        CHECKPOINT_FREQ = Integer.parseInt(args[2]);
        isReady = "passive".equals(REPLICA_MODE); // Passive servers are always ready
        // if (REPLICA_MODE.equals("passive")) {
        //     //isReady = "passive".equals(REPLICA_MODE);
        //     //currentPrimary = args[0]; // The primary serverId given
        // } else {
        //     if (!args[0].equalsIgnoreCase("S1")){
        //         Utilities.printWarning("Waiting on primary for the checkpoint");
        //     }
        // }


        Server server = new Server();

        Server.connectToOtherServer(args[3]);
        Server.connectToOtherServer(args[4]);
        Server.keepCheckServers(args[3]);
        Server.keepCheckServers(args[4]);
        myIp = args[5];

        otherServersConnected.put(args[3], false);
        otherServersConnected.put(args[4], false);

        if ("active".equals(REPLICA_MODE)) {
            isReady = true;
        }
        new Thread(Server::sendCheckpointData).start();

        server.runServer();
        // Connect to other servers
        if ("passive".equals(REPLICA_MODE)) {
        //     if (SERVER_ID.equals(currentPrimary)) {
        //         // Server.connectToOtherServer(args[3]);
        //         // Server.connectToOtherServer(args[4]);
        //         new Thread(Server::sendCheckpointData).start();
            try {
                // Pause for 5 seconds (5000 milliseconds)
                System.out.println("Sleeping for 15 seconds...");
                Thread.sleep(1000*15);
                System.out.println("Woke up!");
            } catch (InterruptedException e) {
                // This block is executed if the sleep is interrupted
                System.err.println("Sleep was interrupted");
            }
            checkPrimaryStatus();
        }
        // }else if ("active".equals(REPLICA_MODE)) {
        //     if ("S1".equals(SERVER_ID)) {
        //         // Server.connectToOtherServer(args[3]);
        //         // Server.connectToOtherServer(args[4]);
        //         isReady = true; // Primary server in active mode is always ready
        //         new Thread(Server::sendCheckpointData).start();
        //     }
        // }
        // Start checkpoint thread only for primary replica in passive mode
        // if (SERVER_ID.equals(currentPrimary) && "passive".equals(REPLICA_MODE)) {
        //     new Thread(Server::sendCheckpointData).start();
        // }
        //System.out.println("finish run main");
    }
}