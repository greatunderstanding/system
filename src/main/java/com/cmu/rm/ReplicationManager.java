package com.cmu.rm;

import com.cmu.properties.Config;
import com.cmu.util.Utilities;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Arrays;

/**
 *
 * @author Temitope Oguntade
 */
public class ReplicationManager {

    private List<String> membership;
    private int member_count;
    private ServerSocket serverSocket;

    public ReplicationManager(int port) throws IOException {
        membership = new ArrayList<>();
        serverSocket = new ServerSocket(port);
    }

    public void listenForGFD() throws IOException {
        Utilities.printState("RM: " + member_count + " members");

        while (true) {
            try (Socket gfdSocket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(gfdSocket.getInputStream()))) {

                String message;
                while ((message = reader.readLine()) != null) {
                    processMembershipChange(message);
                }

            } catch (IOException e) {
                Utilities.printError("RM Error: " + e.getMessage());
            }
        }
    }

    private void processMembershipChange(String message) {
        // Expecting message in the format: "membership [S1, S2] member_count 2"
        if (message.startsWith("membership")) {
            String[] parts = message.split("member_count");
            // parts[0] will contain "membership [S1, S2] "
            // parts[1] will contain " 2"

            String membershipList = parts[0].substring("membership ".length()).trim();
            membershipList = membershipList.substring(1, membershipList.length() - 1); // Remove the brackets
            System.out.println("raw member count: " + parts[1].trim());
            member_count = Integer.parseInt(parts[1].trim());
            System.out.println("updated member count: " + member_count);

            // Update the membership list
            String[] members = membershipList.isEmpty() ? new String[0] : membershipList.split(", ");
            membership = Arrays.stream(members).collect(Collectors.toList());

            printMembership();
        }
    }

    private void printMembership() {
        Utilities.printState("RM: " + member_count + " members: " + String.join(", ", membership));
    }

    public static void main(String[] args) {
        int port = Config.RM_LISTEN_PORT; // Ensure this is the correct port
        try {
            ReplicationManager rm = new ReplicationManager(port);
            rm.listenForGFD();
        } catch (IOException e) {
            Utilities.printError("Failed to start the Replication Manager: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
