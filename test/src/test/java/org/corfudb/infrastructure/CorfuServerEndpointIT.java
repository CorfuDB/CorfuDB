package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;
import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

/**
 * Contains the integration tests for different endpoints and network interfaces
 * Created by cgudisagar on 2/5/23.
 */
@Slf4j
public class CorfuServerEndpointIT extends AbstractIT {
    private static final int PORT_INT_9000 = 9000;
    private static final String LOCALHOST_ENDPOINT_URL = "localhost:9000";

    private Process runCorfuServerWithNetworkInterface(String networkInterface, NetworkInterfaceVersion networkInterfaceVersion) throws IOException {
        return new CorfuServerRunner()
                .setPort(PORT_INT_9000)
                .setNetworkInterface(networkInterface)
                .setNetworkInterfaceVersion(networkInterfaceVersion)
                .setDisableHost(true)
                .setSingle(true)
                .runServer();
    }

    private Process runCorfuServerWithAddress(String address, NetworkInterfaceVersion networkInterfaceVersion) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(PORT_INT_9000)
                .setNetworkInterfaceVersion(networkInterfaceVersion)
                .setSingle(true)
                .runServer();
    }

    private String getLocalNetworkInterface() throws SocketException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (!inetAddress.isLoopbackAddress()) {
                    continue;
                }
                if (inetAddress.getHostAddress().contains(".") || inetAddress.getHostAddress().contains(":")) {
                    return networkInterface.getName();
                }
            }
        }

        return null;
    }

    private String getVersionSpecificNetworkAddress(NetworkInterfaceVersion networkInterfaceVersion) throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            // Find an address that matches with the version.
            while (addresses.hasMoreElements()) {
                InetAddress inetAddress = addresses.nextElement();
                if (inetAddress.isLoopbackAddress()) {
                    continue;
                }
                if (inetAddress instanceof Inet6Address && networkInterfaceVersion == NetworkInterfaceVersion.IPV6) {
                    return inetAddress.getHostAddress();
                } else if (inetAddress instanceof Inet4Address && networkInterfaceVersion == NetworkInterfaceVersion.IPV4) {
                    System.out.println(inetAddress.getHostAddress());
                    return inetAddress.getHostAddress();
                }
            }
        }
        return null;
    }

    /**
     * Test that Corfu Server runs when only endpoint address is configured.
     * (No network interface)
     *
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testEndpointOnly() throws IOException, InterruptedException {
        testEndpointOnlyHelper(NetworkInterfaceVersion.IPV4);
        testEndpointOnlyHelper(NetworkInterfaceVersion.IPV6);
    }

    public static String getLoopbackAddress(NetworkInterfaceVersion version) throws SocketException {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress()) {
                        continue;
                    }
                    if (version == NetworkInterfaceVersion.IPV4 && inetAddress.getHostAddress().contains(".")) {
                        return inetAddress.getHostAddress();
                    } else if (version == NetworkInterfaceVersion.IPV6 && inetAddress.getHostAddress().contains(":")) {
                        return inetAddress.getHostAddress();
                    }
                }
            }

        return null;
    }

    /**
     * Helper method to test that Corfu Server runs when only endpoint address is configured.
     * (No network interface)
     */
    private void testEndpointOnlyHelper(NetworkInterfaceVersion networkInterfaceVersion) throws IOException, InterruptedException {
        String address = getLoopbackAddress(networkInterfaceVersion);
        if (address != null) {
            Process corfuServerProcess = runCorfuServerWithAddress(address, networkInterfaceVersion);
            CorfuRuntime corfuRuntime = createRuntime(getVersionFormattedEndpointURL(address, PORT_INT_9000));
            assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
            corfuRuntime.shutdown();
        }
    }

    /**
     * Test that Corfu Server runs when localhost endpoint is used.
     *
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testLocalHostEndpoint() throws IOException, InterruptedException {
        Process corfuServerProcess = runPersistentServer("localhost", PORT_INT_9000, true);
        CorfuRuntime corfuRuntime = createRuntime(LOCALHOST_ENDPOINT_URL);
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Test that Corfu Server runs when only network interface is configured.
     * (No endpoint (host+port))
     *
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testValidNetworkInterfaces() throws IOException, InterruptedException {
        testValidNetworkInterfacesHelper(NetworkInterfaceVersion.IPV4);
        testValidNetworkInterfacesHelper(NetworkInterfaceVersion.IPV6);
    }

    /**
     * Helper method to test that Corfu Server runs when only network interface is configured.
     * (No endpoint (host+port))
     */
    public void testValidNetworkInterfacesHelper(NetworkInterfaceVersion networkInterfaceVersion) throws IOException, InterruptedException {
        String networkInterfaceName = getLocalNetworkInterface();
        log.info("testValidNetworkInterfacesHelper: Interface from getLocalNetworkInterface {}", networkInterfaceName);

        // get an address from the underlying interfaces
        // (checks for the exact interface and then returns a suitable one)
        String address = getAddressFromInterfaceName(networkInterfaceName, networkInterfaceVersion);
        log.info("testValidNetworkInterfacesHelper: Address from getAddressFromInterfaceName {}", address);
        Process corfuServerProcess = runCorfuServerWithNetworkInterface(networkInterfaceName, networkInterfaceVersion);
        CorfuRuntime corfuRuntime = createRuntime(getVersionFormattedEndpointURL(address, PORT_INT_9000));
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Test that Corfu Server runs with available interfaces
     * when INVALID network interface is used.
     * Server picks up any known of the interfaces (eth0,en0,etc.) and connects to them.
     *
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testInvalidNetworkInterface() throws IOException, InterruptedException {
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV4);
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV6);

    }

    public void testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion networkInterfaceVersion) throws IOException, InterruptedException {
        Process corfuServerProcess = runCorfuServerWithNetworkInterface("INVALID", networkInterfaceVersion);

        // get an address from the underlying interfaces
        // (checks for the exact interface and then returns a suitable one)
        String address = getAddressFromInterfaceName("INVALID", networkInterfaceVersion);
        log.info("testInvalidNetworkInterface: Address from getAddressFromInterfaceName {}", address);
        CorfuRuntime corfuRuntime = createRuntime(getVersionFormattedEndpointURL(address, PORT_INT_9000));
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Helper method to test that Corfu Server runs when only port is configured.
     * (No host address, no network interface)
     *
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Test
    public void testPortOnlyMode() throws IOException, InterruptedException {
        Process corfuServerProcess = new CorfuServerRunner()
                .setPort(PORT_INT_9000)
                .setDisableHost(true)
                .setSingle(true)
                .runServer();
        CorfuRuntime corfuRuntime = createRuntime(LOCALHOST_ENDPOINT_URL);
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }
}
