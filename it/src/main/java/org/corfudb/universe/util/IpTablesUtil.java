package org.corfudb.universe.util;

/**
 * Iptables wrapper/utility class
 */
public class IpTablesUtil {

    /**
     * Drop input packages for a particular ip address
     *
     * @param ipAddress ip address to drop packages
     * @return command line
     */
    public static String[] dropInput(String ipAddress) {
        return new String[] {"iptables", "-A", "INPUT", "-s", ipAddress, "-j", "DROP"};
    }

    /**
     * Drop output packages for a particular ip address
     *
     * @param ipAddress ip address to drop packages
     * @return command line
     */
    public static String[] dropOutput(String ipAddress) {
        return new String[] {"iptables", "-A", "OUTPUT", "-d", ipAddress, "-j", "DROP"};
    }

    /**
     * Recover the drop input rule
     *
     * @param ipAddress ip address to recover the drop input rule
     * @return command line
     */
    public static String[] revertDropInput(String ipAddress) {
        return new String[] {"iptables", "-D", "INPUT", "-s", ipAddress, "-j", "DROP"};
    }

    /**
     * Recover the drop input rule
     *
     * @param ipAddress ip address to recover the drop input rule
     * @return command line
     */
    public static String[] revertDropOutput(String ipAddress) {
        return new String[] {"iptables", "-D", "OUTPUT", "-d", ipAddress, "-j", "DROP"};
    }

    /**
     * Clean all input rules
     *
     * @return command line
     */
    public static String[] cleanInput() {
        return new String[]{"iptables", "-F", "INPUT"};
    }

    /**
     * Clean all output rules
     *
     * @return command line
     */
    public static String[] cleanOutput() {
        return new String[]{"iptables", "-F", "OUTPUT"};
    }

    /**
     * Clean all rules
     *
     * @return command line
     */
    public static String cleanAll() {
        return "iptables -F";
    }
}
