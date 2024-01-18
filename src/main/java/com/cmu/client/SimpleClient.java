package com.cmu.client;

import com.cmu.properties.Config;
import com.cmu.util.Utilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 
 * @author Temitope Oguntade
 */

public class SimpleClient {

    static int request_num = 0;
    private static final Set<Integer> lastTenRequests = new LinkedHashSet<>();

    public static void main(String[] args) {
        if (args.length < 5) {
            Utilities.printError(
                    "Please provide Client ID, heartbeat frequency, and server IPs (e.g., 'C1 1000 172.26.0.116 192.168.1.102 192.168.1.103'). Exiting.");
            return;
        }

        String clientId = args[0];
        int frequency = Integer.parseInt(args[1]);

        for (int i = 2; i < args.length; i++) {
            String serverAddress = args[i];
            new Thread(() -> connectToServer(clientId, frequency, serverAddress)).start();
        }
    }

    private static void connectToServer(String clientId, int frequency, String serverAddress) {
        while (true) {
            try (
                Socket clientSocket = new Socket(serverAddress, Config.SERVER_LISTEN_PORT);
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))
            ) {
                Utilities.printInfo("Connected to server at " + serverAddress + ". Sending messages at " + frequency + "ms intervals.");
                
                // Exchange IDs
                out.println(clientId);
                String serverId = in.readLine();
                //String dummy = in.readLine();
                //System.out.println("this is the dummy id: "+ dummy);
                while (true) {
                    request_num++;

                    // Send request to server
                    Utilities.printSend("<" + clientId + ", " + serverId + ", " + request_num + " request>");
                    out.println(request_num);

                    // Receive response from server
                    String serverResponse = in.readLine();
                    if(!serverResponse.equalsIgnoreCase("IGNORE")){
                    if (serverResponse == null) {
                        Utilities.printError(clientId + " Server at " + serverAddress + " disconnected");
                        break;  // break inner loop and attempt to reconnect
                    }
                    
                    // Log server response and handle duplicate suppression
                    String[] response = serverResponse.split(":");
                    int receivedRequestNum = Integer.valueOf(response[0]);
                    if (lastTenRequests.contains(receivedRequestNum)) {
                        Utilities.printWarning("Response:" + receivedRequestNum + ": Discarded duplicate reply from " + serverId + ".");
                    } else {
                        if (lastTenRequests.size() >= 10) {
                            // Remove the oldest request number to maintain size
                            lastTenRequests.remove(lastTenRequests.iterator().next());
                        }
                        lastTenRequests.add(receivedRequestNum);
                        Utilities.printReceive("<" + clientId + ", " + serverId + ", " + serverResponse + " reply>");
                    }
                    }
                    // Sleep for the specified interval
                    try {
                        Thread.sleep(frequency);
                    } catch (InterruptedException e) {
                        Utilities.printError(clientId + ": Interrupted while waiting to send next request.");
                    }
                
                }

            } catch (Exception e) {
                // Log the exception and prepare for reconnection
                Utilities.printError(clientId + ": Lost connection to " + serverAddress + " - " + e.getMessage());
            }

            try {
                Utilities.printInfo(clientId + ": Attempting to reconnect to " + serverAddress + " in " + frequency + "ms");
                Thread.sleep(frequency);
            } catch (InterruptedException ie) {
                Utilities.printError(clientId + ": Interrupted during reconnection wait.");
            }
        }
    }
}
