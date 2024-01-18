package com.cmu.util;

import com.cmu.properties.Config;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Temitope Oguntade
 */
public class Utilities {

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_WHITE = "\u001B[37m";

    // Background colors
    private static final String ANSI_RED_BACKGROUND = "\u001B[41m";
    private static final String ANSI_GREEN_BACKGROUND = "\u001B[42m";
    private static final String ANSI_YELLOW_BACKGROUND = "\u001B[43m";
    private static final String ANSI_BLUE_BACKGROUND = "\u001B[44m";
    private static final String ANSI_PURPLE_BACKGROUND = "\u001B[45m";
    private static final String ANSI_CYAN_BACKGROUND = "\u001B[46m";
    private static final String ANSI_WHITE_BACKGROUND = "\u001B[47m";

    public static void printMessage(String type, String message) {
        String color;
        switch (type.toUpperCase()) {
            case "INFO":
                color = Config.PRINT_WITH_BG ? ANSI_GREEN_BACKGROUND : ANSI_GREEN;
                break;
            case "WARNING":
                color = Config.PRINT_WITH_BG ? ANSI_YELLOW_BACKGROUND : ANSI_YELLOW;
                break;
            case "ERROR":
                color = Config.PRINT_WITH_BG ? ANSI_RED_BACKGROUND : ANSI_RED;
                break;
            case "LOG":
                color = Config.PRINT_WITH_BG ? ANSI_PURPLE_BACKGROUND : ANSI_PURPLE;
                break;
            case "HEARTBEAT":
                color = Config.PRINT_WITH_BG ? ANSI_CYAN_BACKGROUND : ANSI_CYAN;
                break;
            case "STATE":
                color = Config.PRINT_WITH_BG ? ANSI_BLUE_BACKGROUND : ANSI_BLUE;
                break;
            case "SEND":
                color = Config.PRINT_WITH_BG ? ANSI_WHITE_BACKGROUND : ANSI_WHITE;
                break;
            case "RECEIVE":
                color = Config.PRINT_WITH_BG ? ANSI_GREEN_BACKGROUND : ANSI_GREEN;
                break;
            default:
                color = Config.PRINT_WITH_BG ? ANSI_WHITE_BACKGROUND : ANSI_WHITE;
        }

        int spaceCount = 10 - type.length();
        String spaces = new String(new char[spaceCount]).replace('\0', ' ');

        System.out.println(color + "|" + type + spaces + "|  [" + getCurrentTimestamp() + "]  " + message + ANSI_RESET);
    }

    public static void printInfo(String message) {
        printMessage("INFO", message);
    }

    public static void printWarning(String message) {
        printMessage("WARNING", message);
    }

    public static void printError(String message) {
        printMessage("ERROR", message);
    }

    public static void printLog(String message) {
        printMessage("LOG", message);
    }

    public static void printHeartbeat(String message) {
        printMessage("HEARTBEAT", message);
    }

    public static void printState(String message) {
        printMessage("STATE", message);
    }

    public static void printSend(String message) {
        printMessage("SEND", message);
    }

    public static void printReceive(String message) {
        printMessage("RECEIVE", message);
    }

    public static String getCurrentTimestamp() {
        return (new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()));
    }

    public static void main(String[] args) {
        printInfo("PIRN");
        printWarning("PIRN");
        printError("PIRN");
        printLog("PIRN");
        printHeartbeat("PIRN");
        printState("PIRN");
        printSend("PIRN");
        printReceive("PIRN");
    }

}
