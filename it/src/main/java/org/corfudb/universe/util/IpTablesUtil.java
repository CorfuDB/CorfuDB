package org.corfudb.universe.util;

/**
 * Iptables wrapper/utility class
 */
public class IpTablesUtil {

    /**
     * Drop input packages for a particular ip address
     * @param ipAddress ip address to drop packages
     * @return command line
     */
    public static String dropInput(String ipAddress) {
        return "iptables -A INPUT -s " + ipAddress + " -j DROP";
    }

    /**
     * Drop output packages for a particular ip address
     * @param ipAddress ip address to drop packages
     * @return command line
     */
    public static String dropOutput(String ipAddress) {
        return "iptables -A OUTPUT -d " + ipAddress + " -j DROP";
    }

    /**
     * Clean all input rules
     * @return command line
     */
    public static String[] cleanInput() {
        return new String[]{"iptables", "-F", "INPUT"};
    }

    /**
     * Clean all output rules
     * @return command line
     */
    public static String[] cleanOutput() {
        return new String[]{"iptables", "-F", "OUTPUT"};
    }

    /**
     * Clean all rules
     * @return command line
     */
    public static String cleanAll() {
        return "iptables -F";
    }
}
