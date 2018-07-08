package org.corfudb.util;

import java.net.Inet4Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            for (NetworkInterface networkInterface : getAllInterfaces()) {
                if (networkInterface.isUp()
                        && interfaceName.equals(networkInterface.getName())) {
                    String address;
                    if (networkInterface.isVirtual()) {
                        // If the interface is a subinterface return any ipv4 address
                        address = getIPV4Address(networkInterface.getInterfaceAddresses());
                    } else {
                        // This is not a subinterface, need to exclude the subinterface addresses
                        // from the address list before we select an ipv4 address.
                        Set<InterfaceAddress> parentAddresses = new HashSet<>(networkInterface.getInterfaceAddresses());
                        Set<InterfaceAddress> subInterfacesAddresses = getSubInterfacesAddresses(networkInterface);
                        parentAddresses.removeAll(subInterfacesAddresses);
                        address = getIPV4Address(new ArrayList<>(parentAddresses));
                    }

                    if (address != null) {
                        return address;
                    }

                    break;
                }
            }
        } catch (SocketException e) {
            log.error("Error fetching address from interface name ", e);
            throw new UnrecoverableCorfuError(e);
        }

        throw new UnrecoverableCorfuError("No valid interfaces with name "
                + interfaceName + " found.");
    }

    /**
     * Collects all subinterface addreses for an interface
     */
    private static Set<InterfaceAddress> getSubInterfacesAddresses(NetworkInterface networkInterface) {
        Set<InterfaceAddress> interfaceAddresses = new HashSet<>();
        for (NetworkInterface subInterface : Collections.list(networkInterface.getSubInterfaces())) {
            interfaceAddresses.addAll(subInterface.getInterfaceAddresses());
        }
        return interfaceAddresses;
    }

    /**
     * Finds an ipv4 address in a list of interface addresses
     */
    private static String getIPV4Address(List<InterfaceAddress> addresses) {
        for (InterfaceAddress interfaceAddress : addresses) {
            if (interfaceAddress.getAddress() instanceof Inet4Address) {
                return interfaceAddress.getAddress().getHostAddress();
            }
        }
        return null;
    }

    /**
     * Get all interfaces, including virtual interfaces.
     * @return A list of all interfaces
     * @throws SocketException
     */
    private static List<NetworkInterface> getAllInterfaces() throws SocketException {
        List<NetworkInterface> allInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces())
                .stream()
                .flatMap(m -> {
                    List<NetworkInterface> l = Collections.list(m.getSubInterfaces());
                    l.add(m);
                    return l.stream();
                })
                .collect(Collectors.toList());
        return allInterfaces;
    }
}
