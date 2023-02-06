package org.corfudb.util;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;

/**
 * Network utility methods.
 *
 * <p>Created by zlokhandwala on 3/6/18.
 */
@Slf4j
public class NetworkUtils {

    private NetworkUtils() {
        // prevent instantiation of this class
    }

    /**
     * Fetches the IP address given an interface name.
     * Throws an unrecoverable error and aborts if the server is unable to find a valid address.
     *
     * @param interfaceName Network interface name.
     * @return address for the interface name.
     */
    public static String getAddressFromInterfaceName(String interfaceName, NetworkInterfaceVersion networkInterfaceVersion) {
        NetworkInterfaceVersion currentNetworkInterfaceVersion = networkInterfaceVersion;
        try {
            List<NetworkInterface> interfaceList = getAllInterfaces();
            List<String> genericInterfaces = Arrays.asList("eth0", "en0", "eth", "en", "lo");
            String returnAddress;
            for (int i = 0; i < 2; i++) {
                // First try to find an address in the given network interface
                for (NetworkInterface networkInterface : interfaceList) {
                    if (networkInterface.isUp()
                            && interfaceName.equals(networkInterface.getName())) {
                        returnAddress = getAddressFromInterface(networkInterface, currentNetworkInterfaceVersion);
                        if (returnAddress != null) {
                            return returnAddress;
                        }
                        break;
                    }
                }

                // if no address is found, search generic interfaces like eth0/en0 etc.
                for (String genericInterface : genericInterfaces) {
                    for (NetworkInterface networkInterface : interfaceList) {
                        if (networkInterface.getName().startsWith(genericInterface)
                                && networkInterface.isUp()
                                && !interfaceName.equals(networkInterface.getName())) {
                            returnAddress = getAddressFromInterface(networkInterface, currentNetworkInterfaceVersion);
                            if (returnAddress != null) {
                                return returnAddress;
                            }
                        }
                    }
                }

                if (i==0) {
                    // The specified version's address was not found in the first iteration.
                    // Retry after flipping the version.
                    currentNetworkInterfaceVersion =
                            currentNetworkInterfaceVersion == NetworkInterfaceVersion.IPV6 ?
                                    NetworkInterfaceVersion.IPV4 :
                                    NetworkInterfaceVersion.IPV6;
                }
            }
        } catch (SocketException e) {
            log.error("Error fetching address from interface name ", e);
            throw new UnrecoverableCorfuError(e);
        }

        throw new UnrecoverableCorfuError("No valid ip addresses found for interface: "
                + interfaceName);
    }

    private static String getAddressFromInterface(NetworkInterface networkInterface, NetworkInterfaceVersion networkInterfaceVersion) {
        String address;
        if (networkInterface.isVirtual()) {
            // If the interface is a subinterface return any ip address
            address = getIPAddress(networkInterface.getInterfaceAddresses(), networkInterfaceVersion);
        } else {
            // This is not a subinterface, need to exclude the subinterface addresses
            // from the address list before we select an ip address.
            Set<InterfaceAddress> parentAddresses = new HashSet<>(networkInterface.getInterfaceAddresses());
            Set<InterfaceAddress> subInterfacesAddresses = getSubInterfacesAddresses(networkInterface);
            parentAddresses.removeAll(subInterfacesAddresses);
            address = getIPAddress(new ArrayList<>(parentAddresses), networkInterfaceVersion);
        }
        return address;
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
     * Finds an ipv4 or ipv6 address in a list of interface addresses
     * based on the Server arguments
     */
    private static String getIPAddress(List<InterfaceAddress> addresses, NetworkInterfaceVersion networkInterfaceVersion) {
        for (InterfaceAddress interfaceAddress : addresses) {
            if (interfaceAddress.getAddress() instanceof Inet6Address && networkInterfaceVersion == NetworkInterfaceVersion.IPV6) {
                return getVersionFormattedHostAddress(interfaceAddress.getAddress().getHostAddress());
            } else if (interfaceAddress.getAddress() instanceof Inet4Address && networkInterfaceVersion == NetworkInterfaceVersion.IPV4) {
                return getVersionFormattedHostAddress(interfaceAddress.getAddress().getHostAddress());
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
        return Collections.list(NetworkInterface.getNetworkInterfaces())
                .stream()
                .flatMap(m -> {
                    List<NetworkInterface> l = Collections.list(m.getSubInterfaces());
                    l.add(m);
                    return l.stream();
                })
                .collect(Collectors.toList());
    }

}
