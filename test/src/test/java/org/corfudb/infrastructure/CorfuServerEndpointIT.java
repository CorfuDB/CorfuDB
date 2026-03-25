package org.corfudb.infrastructure;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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
     * Log all available network interfaces and their addresses for debugging.
     */
    private void logAllNetworkInterfaces() throws SocketException {
        log.info("=== BEGIN: All Network Interfaces ===");
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            log.info("Interface: name={}, displayName={}, isUp={}, isLoopback={}, isVirtual={}",
                    ni.getName(), ni.getDisplayName(), ni.isUp(), ni.isLoopback(), ni.isVirtual());
            
            List<InterfaceAddress> interfaceAddresses = ni.getInterfaceAddresses();
            for (InterfaceAddress ia : interfaceAddresses) {
                InetAddress addr = ia.getAddress();
                String addrType = addr instanceof Inet4Address ? "IPv4" : 
                                  addr instanceof Inet6Address ? "IPv6" : "Unknown";
                log.info("  Address: {} (type={}, isLoopback={}, isLinkLocal={}, isSiteLocal={})",
                        addr.getHostAddress(), addrType, addr.isLoopbackAddress(),
                        addr.isLinkLocalAddress(), addr.isSiteLocalAddress());
            }
        }
        log.info("=== END: All Network Interfaces ===");
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
        // Log all network interfaces for debugging CI issues
        logAllNetworkInterfaces();
        
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV4, PORT_INT_9000);
        testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion.IPV6, PORT_INT_9001);

    }

    public void testInvalidNetworkInterfaceHelper(NetworkInterfaceVersion networkInterfaceVersion, int port) throws Exception {
        log.info("testInvalidNetworkInterface: Starting test for {} on port {}", networkInterfaceVersion, port);
        
        // Resolve the fallback address first. If no valid address exists for this
        // network interface version (e.g., no IPv6 on the host), the server would
        // crash with the same error, so skip this variant.
        String address;
        try {
            address = getAddressFromInterfaceName("INVALID", networkInterfaceVersion);
            log.info("testInvalidNetworkInterface: getAddressFromInterfaceName('INVALID', {}) returned: {}",
                    networkInterfaceVersion, address);
        } catch (UnrecoverableCorfuError e) {
            log.warn("testInvalidNetworkInterface: Skipping {} - no valid fallback address available",
                    networkInterfaceVersion, e);
            return;
        }

        // Log address details for debugging
        String cleanAddress = address.replace("[", "").replace("]", "");
        InetAddress inetAddr = InetAddress.getByName(cleanAddress);
        log.info("testInvalidNetworkInterface: Resolved address details - address={}, isLoopback={}, " +
                "isLinkLocal={}, isSiteLocal={}, isAnyLocal={}, isReachable(1000ms)={}",
                inetAddr.getHostAddress(), inetAddr.isLoopbackAddress(),
                inetAddr.isLinkLocalAddress(), inetAddr.isSiteLocalAddress(),
                inetAddr.isAnyLocalAddress(), inetAddr.isReachable(1000));

        log.info("testInvalidNetworkInterface: Proceeding with address {} for {}", address, networkInterfaceVersion);
        Process corfuServerProcess = runCorfuServerWithNetworkInterface("INVALID", networkInterfaceVersion, port);
        log.info("testInvalidNetworkInterface: Server process started, PID={}, isAlive={}", 
                corfuServerProcess.pid(), corfuServerProcess.isAlive());

        // Wait briefly for the server to start (or crash) before attempting to connect
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        log.info("testInvalidNetworkInterface: After sleep, server isAlive={}", corfuServerProcess.isAlive());
        
        if (!corfuServerProcess.isAlive()) {
            int exitCode = corfuServerProcess.exitValue();
            log.error("testInvalidNetworkInterface: Server process exited prematurely for {}, exitValue={}",
                    networkInterfaceVersion, exitCode);
            
            // Print server console log for debugging
            String serverLogPath = CORFU_LOG_PATH + File.separator + address.replace("[", "").replace("]", "") 
                    + "_" + port + "_consolelog";
            try {
                Path logFile = Path.of(serverLogPath);
                if (Files.exists(logFile)) {
                    String logContent = Files.readString(logFile);
                    log.error("Server console log ({}):\n{}", serverLogPath, logContent);
                } else {
                    log.warn("Server console log not found at: {}", serverLogPath);
                }
            } catch (IOException e) {
                log.warn("Failed to read server console log: {}", e.getMessage());
            }
            
            fail("Server process exited prematurely for " + networkInterfaceVersion + " with exit code " + exitCode);
        }

        String endpoint = getVersionFormattedEndpointURL(address, port);
        log.info("testInvalidNetworkInterface: Attempting to connect runtime to endpoint: {}", endpoint);
        CorfuRuntime corfuRuntime = createRuntime(endpoint);
        log.info("testInvalidNetworkInterface: Runtime connected successfully");
        
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        corfuRuntime.shutdown();
        log.info("testInvalidNetworkInterface: Test completed successfully for {}", networkInterfaceVersion);
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
