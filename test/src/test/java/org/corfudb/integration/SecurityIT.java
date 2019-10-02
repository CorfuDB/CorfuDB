package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.test.CorfuServerRunner;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * This test suit exercises the ability to enable TLS on Corfu servers and runtime
 *
 * Created by Sam Behnam on 8/13/18.
 */
@Slf4j
public class SecurityIT extends AbstractIT {
    private String corfuSingleNodeHost;
    private int corfuStringNodePort;
    private String singleNodeEndpoint;

    private boolean tlsEnabled = true;
    private String serverPathToKeyStore;
    private String serverPathToKeyStorePassword;
    private String serverPathToTrustStore;
    private String serverPathToTrustStorePassword;
    private String runtimePathToKeyStore;
    private String runtimePathToKeyStorePassword;
    private String runtimePathToTrustStore;
    private String runtimePathToTrustStorePassword;

    /* A helper method that start a single TLS enabled server and returns a process. */
    private Process runSinglePersistentServerTls() throws IOException {
        return new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuStringNodePort)
                .setTlsEnabled(tlsEnabled)
                .setKeyStore(serverPathToKeyStore)
                .setKeyStorePassword(serverPathToKeyStorePassword)
                .setTrustStore(serverPathToTrustStore)
                .setTrustStorePassword(serverPathToTrustStorePassword)
                .setLogPath(CorfuServerRunner.getCorfuServerLogPath(corfuSingleNodeHost, corfuStringNodePort))
                .setSingle(true)
                .runServer();
    }

    /**
     * This is a helper method that loads the properties required for creating corfu servers before
     * each test by converting them from the values provided in CorfuDB.properties. Note that it
     * will throw {@link NumberFormatException} if the numerical properties provided in CorfuDB.properties
     * are not well-formed. If the CorfuDB.properties has invalid or non existent values for the keystore or
     * truststore, it will lead to throwing {@link IllegalArgumentException}.
     */
    @Before
    public void loadProperties() {
        // Load host and port properties
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));

        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);

        // Load TLS configuration, keystore and truststore properties
        serverPathToKeyStore = getPropertyAbsolutePath("serverPathToKeyStore");
        serverPathToKeyStorePassword = getPropertyAbsolutePath("serverPathToKeyStorePassword");
        serverPathToTrustStore = getPropertyAbsolutePath("serverPathToTrustStore");
        serverPathToTrustStorePassword = getPropertyAbsolutePath("serverPathToTrustStorePassword");
        runtimePathToKeyStore = getPropertyAbsolutePath("runtimePathToKeyStore");
        runtimePathToKeyStorePassword = getPropertyAbsolutePath("runtimePathToKeyStorePassword");
        runtimePathToTrustStore = getPropertyAbsolutePath("runtimePathToTrustStore");
        runtimePathToTrustStorePassword = getPropertyAbsolutePath("runtimePathToTrustStorePassword");
    }

    /**
     * Take the property string provided int CorfuDB.properties and return the absolute path string. Throws
     * {@link IllegalArgumentException} if the the provided path is non existent or invalid.
     *
     * @param pathProperty a property in CorfuDB.properties file pointing to a file whose absolute path
     *                     required to be loaded.
     * @return absolute path provided in CorfuDB.properties for the pathProperty
     */
    private String getPropertyAbsolutePath(String pathProperty) {
        if ((PROPERTIES.getProperty(pathProperty)) == null ||
            Files.notExists(Paths.get(PROPERTIES.getProperty(pathProperty)).toAbsolutePath())) {
            throw new IllegalArgumentException(
                    String.format("CorfuDB.properties contains invalid or non existent value for :%s",
                    pathProperty));
        }

        return Paths.get(PROPERTIES.getProperty(pathProperty)).toAbsolutePath().toString();
    }

    /**
     * This test creates Corfu runtime and a single Corfu server according to the configuration
     * provided in CorfuDB.properties. Corfu runtime configures TLS related parameters using
     * {@link CorfuRuntime}'s API and then asserts that operations on a CorfuTable is executed
     * as Expected.
     *
     * @throws Exception
     */
    @Test
    public void testServerRuntimeTlsEnabledMethod() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServerTls();

        // Start a Corfu runtime
        runtime = new CorfuRuntime(singleNodeEndpoint)
                                    .enableTls(runtimePathToKeyStore,
                                               runtimePathToKeyStorePassword,
                                               runtimePathToTrustStore,
                                               runtimePathToTrustStorePassword)
                                    .setCacheDisabled(true)
                                    .connect();

        // Create CorfuTable
        CorfuTable testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, Object>>() {})
                .setStreamName("volbeat")
                .open();

        // CorfuTable stats before usage
        final int initialSize = testTable.size();

        // Put key values in CorfuTable
        final int count = 100;
        final int entrySize = 1000;
        for (int i = 0; i < count; i++) {
            testTable.put(String.valueOf(i), new byte[entrySize]);
        }

        // Assert that put operation was successful
        final int sizeAfterPuts = testTable.size();
        assertThat(sizeAfterPuts).isGreaterThanOrEqualTo(initialSize);
        log.info("Initial Table Size: {} - FinalTable Size:{}", initialSize, sizeAfterPuts);

        // Assert that table has correct size (i.e. count) and and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test creates Corfu runtime and a single Corfu server according to the configuration
     * provided in CorfuDB.properties. Corfu runtime configures TLS related parameters using
     * {@link org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters} and then asserts that
     * operations on a CorfuTable is executed as Expected.
     *
     * @throws Exception
     */
    @Test
    public void testServerRuntimeTlsEnabledByParameter() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServerTls();

        // Create Runtime parameters for enabling TLS
        final CorfuRuntime.CorfuRuntimeParameters runtimeParameters = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .layoutServer(NodeLocator.parseString(singleNodeEndpoint))
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .build();

        // Start a Corfu runtime from parameters
        runtime = CorfuRuntime
                .fromParameters(runtimeParameters)
                .connect();

        // Create CorfuTable
        CorfuTable testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, Object>>() {})
                .setStreamName("volbeat")
                .open();

        // CorfuTable stats before usage
        final int initialSize = testTable.size();

        // Put key values in CorfuTable
        final int count = 100;
        final int entrySize = 1000;
        for (int i = 0; i < count; i++) {
            testTable.put(String.valueOf(i), new byte[entrySize]);
        }

        // Assert that put operation was successful
        final int sizeAfterPuts = testTable.size();
        assertThat(sizeAfterPuts).isGreaterThanOrEqualTo(initialSize);
        log.info("Initial Table Size: {} - FinalTable Size:{}", initialSize, sizeAfterPuts);

        // Assert that table has correct size (i.e. count) and and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Testing that configuring incorrect TLS parameters will lead to throwing
     * {@link UnrecoverableCorfuError} exception.
     *
     * @throws Exception
     */
    @Test(expected = UnrecoverableCorfuError.class)
    public void testIncorrectKeyStoreException() throws Exception {
        // Run a corfu server with incorrect truststore
        runSinglePersistentServerTls();

        // Start a Corfu runtime with incorrect truststore
        final CorfuRuntime.CorfuRuntimeParameters runtimeParameters = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .layoutServer(NodeLocator.parseString(singleNodeEndpoint))
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToKeyStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .build();

        // Connecting to runtime
        runtime = CorfuRuntime.fromParameters(runtimeParameters).connect();
    }
}
