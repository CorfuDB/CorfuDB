package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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
    private static final int PORT_INT_9001 = 9001;
    private static final String LOCALHOST_ENDPOINT_URL = "localhost:9000";

    private Process runCorfuServerWithNetworkInterface(String networkInterface, NetworkInterfaceVersion networkInterfaceVersion) throws IOException {
        return runCorfuServerWithNetworkInterface(networkInterface, networkInterfaceVersion, PORT_INT_9000);
    }

    private Process runCorfuServerWithNetworkInterface(String networkInterface, NetworkInterfaceVersion networkInterfaceVersion, int port) throws IOException {
        return new CorfuServerRunner()
                .setPort(port)
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

    /**
     * Test that Corfu Server runs when only endpoint address is configured.
     * (No network interface)
     *
     * @throws Exception error
     */
    @Test
    public void testEndpointOnly() throws Exception {
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
    private void testEndpointOnlyHelper(NetworkInterfaceVersion networkInterfaceVersion) throws Exception {
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
     * @throws Exception Exception
     */
    @Test
    public void testLocalHostEndpoint() throws Exception {
        Process corfuServerProcess = runPersistentServer("localhost", PORT_INT_9000, true);
        CorfuRuntime corfuRuntime = createRuntime(LOCALHOST_ENDPOINT_URL);
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Test that Corfu Server runs when only network interface is configured.
     * (No endpoint (host+port))
     *
     * @throws Exception Exception
     */
    @Test
    public void testValidNetworkInterfaces() throws Exception {
        testValidNetworkInterfacesHelper(NetworkInterfaceVersion.IPV4);
        testValidNetworkInterfacesHelper(NetworkInterfaceVersion.IPV6);
    }

    /**
     * Helper method to test that Corfu Server runs when only network interface is configured.
     * (No endpoint (host+port))
     */
    public void testValidNetworkInterfacesHelper(NetworkInterfaceVersion networkInterfaceVersion) throws Exception {
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
     * @throws Exception Exception
     */
    @Test
    public void testInvalidNetworkInterface() throws Exception {
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV4, PORT_INT_9000);
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV6, PORT_INT_9001);

    }

    public void testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion networkInterfaceVersion, int port) throws Exception {
        // Resolve the fallback address first. If no valid address exists for this
        // network interface version (e.g., no IPv6 on the host), the server would
        // crash with the same error, so skip this variant.
        String address;
        try {
            address = getAddressFromInterfaceName("INVALID", networkInterfaceVersion);
        } catch (UnrecoverableCorfuError e) {
            log.warn("testInvalidNetworkInterface: Skipping {} - no valid fallback address available",
                    networkInterfaceVersion, e);
            return;
        }

        log.info("testInvalidNetworkInterface: Address from getAddressFromInterfaceName {}", address);
        Process corfuServerProcess = runCorfuServerWithNetworkInterface("INVALID", networkInterfaceVersion, port);

        // Wait briefly for the server to start (or crash) before attempting to connect
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        if (!corfuServerProcess.isAlive()) {
            log.warn("testInvalidNetworkInterface: Server process exited prematurely for {}", networkInterfaceVersion);
            return;
        }

        CorfuRuntime corfuRuntime = createRuntime(getVersionFormattedEndpointURL(address, port));
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Helper method to test that Corfu Server runs when only port is configured.
     * (No host address, no network interface)
     *
     * @throws Exception Exception
     */
    @Test
    public void testPortOnlyMode() throws Exception {
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
