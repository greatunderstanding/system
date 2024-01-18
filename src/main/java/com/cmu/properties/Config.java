
package com.cmu.properties;

/**
 *
 * @author Temitope Oguntade
 */
public class Config {
    
    // Server
    public static int SERVER_LISTEN_PORT = 7788;
    public static String SERVER_ADDRESS = "127.0.0.1";
    public static int heartbeatFreq = 5000; // in milliseconds

    // GFD
    public static int GFD_LISTEN_PORT = 7789;
    public static String GFD_ADDRESS = "172.31.61.71";

    // LFD
    public static int LFD_LISTEN_PORT = 7790;
    public static String LFD_ADDRESS = "127.0.0.1";

    // General
    public static boolean PRINT_WITH_BG = true;

    // RM
    public static int RM_LISTEN_PORT = 7792;
    public static String RM_ADDRESS = "172.31.61.71";

}
