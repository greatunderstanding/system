package com.cmu.lfd;

import com.cmu.properties.Config;
import com.cmu.util.Utilities;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;
//import com.sun.management.OperatingSystemMXBean;




import com.fasterxml.jackson.databind.ObjectMapper;



/**
 * The LocalFaultDetector (LFD) is responsible for sending heartbeats to the
 * associated server and to the GlobalFaultDetector (GFD) to indicate its own
 * liveness.
 *
 * @author Temitope Oguntade
 */
public class LocalFaultDetector {

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static String LFD_ID;
    private static String SERVER_ADDRESS;
    private static int SERVER_PORT;
    private static int HEARTBEAT_INTERVAL;
    private static String SERVER_ID = ""; // To store the ID of the server after connection

    // For maintaining a persistent connection with GFD
    private static Socket gfdSocket;
    private static PrintWriter gfdOut;

    private static final List<Double> cpuUsageData = new ArrayList<>();
    private List<Long> memoryUsageData = new ArrayList<>();
    private static final List<long[]> diskUsageData = new ArrayList<>();
    private List<Date> timeData = new ArrayList<>();
    private AtomicInteger lastExportedIndex = new AtomicInteger(0);

    //private static List<MetricsData> metricsList = new ArrayList<>();


    public static void main(String[] args) {

        

        scheduler.scheduleAtFixedRate(LocalFaultDetector::exportToCSV, 0, 1, TimeUnit.SECONDS);


        if (args.length < 4) {
            Utilities.printError("Not enough arguments provided. Please provide heartbeat frequency, LFD_ID, server IP, and server port (e.g., '5000 LFD1 127.0.0.1 7788').");
            return;
        }

        try {
            HEARTBEAT_INTERVAL = Integer.parseInt(args[0]);
            LFD_ID = args[1];
            SERVER_ADDRESS = args[2];
            SERVER_PORT = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            Utilities.printError("Invalid input. Please check the heartbeat frequency and server port.");
            return;
        }

        try {
            gfdSocket = new Socket(Config.GFD_ADDRESS, Config.GFD_LISTEN_PORT);
            gfdOut = new PrintWriter(gfdSocket.getOutputStream(), true);
        } catch (IOException e) {
            Utilities.printError("Failed to establish initial connection to GFD: " + e.getMessage());
            return;
        }

        final HeartbeatTask task = new HeartbeatTask();
        scheduler.scheduleAtFixedRate(task::run, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        final GFDHeartbeatTask gfdHeartbeatTask = new GFDHeartbeatTask();
        scheduler.scheduleAtFixedRate(gfdHeartbeatTask::run, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        
        // Start exporting real-time metrics data to CSV
        

        // You might also want to add a shutdown hook to handle graceful shutdown and notify GFD about it
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gfdOut.println("TERMINATE");
        }));
    }
    private static OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    private static double getCPUUsage() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

        // Check if the operating system supports CPU usage monitoring
            com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
            
            // Get CPU usage as a percentage
            double cpuUsage = sunOsBean.getSystemCpuLoad() * 100.0;
            return cpuUsage;
            
        
        }

        private static long getMemoryUsage() {
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
            return heapMemoryUsage.getUsed(); // Returns used memory in bytes as a long
        }

    private static long[] getDiskUsage(String path) {
        File file = new File(path);
            long freeSpace = file.getFreeSpace();
            long totalSpace = file.getTotalSpace();
            return new long[]{freeSpace, totalSpace};
        }
    private static void exportToCSV() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("metrics_data.csv", true))) {
            String dataLine = getSystemMetrics();
            writer.write(dataLine);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    private static String getSystemMetrics() {
        String timestamp = dateFormat.format(new Date());
        double cpuUsage = getCPUUsage();
        long memoryUsage = getMemoryUsage();
        long[] diskUsage = getDiskUsage("/");
        if (cpuUsage <=50){
            Utilities.printInfo("CPU Usage: " + cpuUsage + "%");
        }else{
            Utilities.printWarning("CPU Usage: " + cpuUsage + "%");
        }
        Utilities.printInfo("Memory Usage: Used=" + memoryUsage + " bytes");
        Utilities.printInfo("Disk Usage: Free=" + diskUsage[0] + " bytes, Total=" + diskUsage[1] + " bytes");


        return String.format("%s,%.2f,%d,%d,%d", timestamp, cpuUsage, memoryUsage, diskUsage[0], diskUsage[1]);
    }
    



        // Getters and setters (if needed)
    

    static class HeartbeatTask {

        private boolean hasNotifiedGFD = false;
        private final AtomicInteger heartbeatCounter = new AtomicInteger(0);

        public void run() {
            int currentHeartbeatCount = heartbeatCounter.incrementAndGet();

                        // Report CPU usage
            // double cpuUsage = getCPUUsage();
            // //Utilities.printInfo("CPU Usage: " + cpuUsage + "%");
            // cpuUsageData.add(cpuUsage);

            // // Report memory usage
            // MemoryUsage memoryUsage = getMemoryUsage();
            // //Utilities.printInfo("Memory Usage: Used=" + memoryUsage.getUsed() + " bytes, Max=" + memoryUsage.getMax() + " bytes");
            // memoryUsageData.add(memoryUsage);

            // // Report disk usage
            // long[] diskUsage = getDiskUsage("/");
            // //Utilities.printInfo("Disk Usage: Free=" + diskUsage[0] + " bytes, Total=" + diskUsage[1] + " bytes");
            // diskUsageData.add(diskUsage);


            try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                if (SERVER_ID.isEmpty()) {
                    out.println("INIT_LFD");
                    out.flush();
                    SERVER_ID = in.readLine(); // Receive Server ID

                    // Send LFD ID to the server
                    out.println(LFD_ID);
                    out.flush();
                }

                Utilities.printHeartbeat("Sending HEARTBEAT " + currentHeartbeatCount + " to " + SERVER_ID + " (Replica Server)...");
                out.println("HEARTBEAT " + currentHeartbeatCount);
                out.flush();

                String response = in.readLine();
                if (response != null && response.contains("ALIVE " + currentHeartbeatCount)) {
                    Utilities.printHeartbeat(response + " from " + SERVER_ID + " (Replica Server)");
                    if (!hasNotifiedGFD) {
                        notifyGFDAddition();
                    }
                } else {
                    Utilities.printHeartbeat("HEARTBEAT " + currentHeartbeatCount + " to " + SERVER_ID + " (Replica Server) FAILED");
                    notifyGFDDeletion();
                }
            } catch (IOException e) {
                Utilities.printHeartbeat("HEARTBEAT " + currentHeartbeatCount + " to " + SERVER_ID + " (Replica Server) FAILED");
                notifyGFDDeletion();
            }
        }
        

        // private CategoryChart createChart(String title, String xAxisTitle, String yAxisTitle, List<Integer> xData, List<? extends Number> yData) {
        //     CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(xAxisTitle).yAxisTitle(yAxisTitle).build();
        //     chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        //     chart.addSeries(title, xData, yData);
        //     return chart;
        // }



        private void notifyGFDAddition() {
            try {
                gfdOut.println(LFD_ID + ": add replica " + SERVER_ID);
                hasNotifiedGFD = true;
                Utilities.printInfo("Notified GFD about the replica server " + SERVER_ID + "(add replica "+SERVER_ID+")");
            } catch (Exception e) {
                Utilities.printError("Failed to notify GFD: " + e.getMessage());
            }
        }

        private void notifyGFDDeletion() {
            if (hasNotifiedGFD) {
                try {
                    gfdOut.println(LFD_ID + ": delete replica " + SERVER_ID);
                    hasNotifiedGFD = false;
                    Utilities.printInfo("Notified GFD to delete the replica server " + SERVER_ID + "(delete replica "+SERVER_ID+")");
                } catch (Exception e) {
                    Utilities.printError("Failed to notify GFD of deletion: " + e.getMessage());
                }
            }
        }
    }
    

    static class GFDHeartbeatTask {
        public void run() {
            try {
                gfdOut.println(LFD_ID + "_HEARTBEAT");
                Utilities.printHeartbeat("Sent HEARTBEAT to GFD.");
            } catch (Exception e) {
                Utilities.printWarning("Failed to send heartbeat to GFD: " + e.getMessage());
            }
        }
    }
}
