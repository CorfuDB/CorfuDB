package org.corfudb.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

/**
 * Network utility methods.
 *
 * <p>Created by zlokhandwala on 3/6/18.
 */
@Slf4j
public class NetworkUtils {

    /**
     * Fetches the IP address given an interface name.
     * Throws an unrecoverable error and aborts if the server is unable to find a valid address.
     *
     * @param interfaceName Network interface name.
     * @return address for the interface name.
     */
    public static String getAddressFromInterfaceName(String interfaceName) {
        try {
            Enumeration<InetAddress> inetAddress = NetworkInterface.getByName(interfaceName)
                    .getInetAddresses();
            InetAddress currentAddress;
            while (inetAddress.hasMoreElements()) {
                currentAddress = inetAddress.nextElement();
                log.info("currentAddress: {}", currentAddress);
                if (currentAddress instanceof Inet4Address) {
                    return currentAddress.getHostAddress();
                }
            }
        } catch (SocketException e) {
            log.error("Error fetching address from interface name ", e);
            throw new UnrecoverableCorfuError(e);
        }
        throw new UnrecoverableCorfuError("No valid interfaces with name "
                + interfaceName + " found.");
    }
}
