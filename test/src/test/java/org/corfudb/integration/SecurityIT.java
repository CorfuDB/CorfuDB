package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.FileWatcher;
import org.corfudb.util.NodeLocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;

/**
 * This test suit exercises the ability to enable TLS on Corfu servers and runtime
 * Created by Sam Behnam on 8/13/18.
 *
 * Member Variable names indexing Info:
 * serverKeyStorePath = key store file path of the server.
 *                      this is the location used by actual server process
 * Backup = Backup file to restore the original file at the end of tests.
 *          Backups are created in the @Before section and deleted in the @After section
 * Temp = Swap files created to copy the file first into a Temp location and then do
 *        an ATOMIC MOVE from Temp to Destination
 */
@Slf4j
public class SecurityIT extends AbstractIT {
    private String corfuSingleNodeHost;
    private int corfuSingleNodePort;
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
    private String disableCertExpiryCheckFile;
    private boolean disableFileWatcher = false;
    private static final short SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 3;
    private Path serverKeyStoreFilePath;

    private Path serverKeyStoreFilePathBackup;
    private Path serverInvalidKeyStorePath;
    private Path serverInvalidKeyStorePathTemp;
    private Path serverKeyStoreFilePathTemp;

    private static final int MAX_RETRY_LIMIT = 5;
    private static final int MIN_RETRY_LIMIT = 3;
    private static final int RETRY_INTERVAL_SECONDS = 1;

    /**
     * A helper method that start a single TLS enabled server and returns a process.
     *
     * @return The process running the Corfu Server
     * @throws IOException when File IO Error occurs while loading certs
     */
    /*  */
    private Process runSinglePersistentServerTls(boolean serverEnableTlsMutualAuth) throws IOException {
        AbstractIT.CorfuServerRunner corfuServerRunner = new AbstractIT.CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .setTlsEnabled(tlsEnabled)
                .setKeyStore(serverPathToKeyStore)
                .setKeyStorePassword(serverPathToKeyStorePassword)
                .setTrustStore(serverPathToTrustStore)
                .setTrustStorePassword(serverPathToTrustStorePassword)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, corfuSingleNodePort))
                .setSingle(true);

        if (disableCertExpiryCheckFile != null) {
            corfuServerRunner.setDisableCertExpiryCheckFile(disableCertExpiryCheckFile);
        }

        corfuServerRunner.setDisableFileWatcher(disableFileWatcher);

        corfuServerRunner.setTlsMutualAuthEnabled(serverEnableTlsMutualAuth);

        return corfuServerRunner.runServer();
    }

    /**
     * This is a helper method that loads the properties required for creating corfu servers before
     * each test by converting them from the values provided in CorfuDB.properties. Note that it
     * will throw {@link NumberFormatException} if the numerical properties provided in CorfuDB.properties
     * are not well-formed. If the CorfuDB.properties has invalid or non-existent values for the keystore or
     * truststore, it will lead to throwing {@link IllegalArgumentException}.
     */
    @Before
    public void loadProperties() throws IOException {
        // Load host and port properties
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuSingleNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));

        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuSingleNodePort);

        // Load TLS configuration, keystore and truststore properties
        serverPathToKeyStore = getPropertyAbsolutePath("serverPathToKeyStore");
        serverPathToKeyStorePassword = getPropertyAbsolutePath("serverPathToKeyStorePassword");
        serverPathToTrustStore = getPropertyAbsolutePath("serverPathToTrustStore");
        serverPathToTrustStorePassword = getPropertyAbsolutePath("serverPathToTrustStorePassword");
        runtimePathToKeyStore = getPropertyAbsolutePath("runtimePathToKeyStore");
        runtimePathToKeyStorePassword = getPropertyAbsolutePath("runtimePathToKeyStorePassword");
        runtimePathToTrustStore = getPropertyAbsolutePath("runtimePathToTrustStore");
        runtimePathToTrustStorePassword = getPropertyAbsolutePath("runtimePathToTrustStorePassword");


        serverKeyStoreFilePath = Paths.get(serverPathToKeyStore);
        serverKeyStoreFilePathBackup = serverKeyStoreFilePath.resolveSibling(serverKeyStoreFilePath.getFileName() + ".backup");

        // Backup Valid Certificates
        Files.copy(serverKeyStoreFilePath, serverKeyStoreFilePathBackup, StandardCopyOption.REPLACE_EXISTING);

        serverInvalidKeyStorePath = Paths.get(getPropertyAbsolutePath("serverPathToInvalidKeyStore"));
        serverInvalidKeyStorePathTemp = serverInvalidKeyStorePath.resolveSibling(serverInvalidKeyStorePath + ".temp");
        serverKeyStoreFilePathTemp = serverKeyStoreFilePathBackup.resolveSibling(serverKeyStoreFilePathBackup + ".temp");

    }

    @After
    public void restoreProperties() throws Exception {
        Files.move(serverKeyStoreFilePathBackup, serverKeyStoreFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(serverKeyStoreFilePathBackup);
        Files.deleteIfExists(serverInvalidKeyStorePathTemp);
        Files.deleteIfExists(serverKeyStoreFilePathTemp);
    }

    /**
     * Take the property string provided int CorfuDB.properties and return the absolute path string. Throws
     * {@link IllegalArgumentException} if the provided path is non-existent or invalid.
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
        final Process corfuServer = runSinglePersistentServerTls(false);

        // Start a Corfu runtime
        CorfuRuntime runtime = new CorfuRuntime(singleNodeEndpoint)
                .enableTls(runtimePathToKeyStore,
                        runtimePathToKeyStorePassword,
                        runtimePathToTrustStore,
                        runtimePathToTrustStorePassword)
                .setCacheDisabled(true)
                .registerSystemDownHandler(getShutdownHandler())
                .connect();

        // Create CorfuTable
        PersistentCorfuTable<String, Object> testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, Object>>() {})
                .setStreamName("volbeat")
                .open();

        // CorfuTable stats before usage
        final int initialSize = testTable.size();

        // Put key values in CorfuTable
        final int count = 100;
        final int entrySize = 1000;
        for (int i = 0; i < count; i++) {
            testTable.insert(String.valueOf(i), new byte[entrySize]);
        }

        // Assert that put operation was successful
        final int sizeAfterPuts = testTable.size();
        assertThat(sizeAfterPuts).isGreaterThanOrEqualTo(initialSize);
        log.info("Initial Table Size: {} - FinalTable Size:{}", initialSize, sizeAfterPuts);

        // Assert that table has correct size (i.e. count) and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test creates Corfu runtime and a single Corfu server according to the configuration
     * provided in CorfuDB.properties. Corfu runtime configures TLS related parameters using
     * {@link CorfuRuntimeParameters} and then asserts that
     * operations on a CorfuTable is executed as Expected.
     *
     * @throws Exception
     */
    @Test
    public void testServerRuntimeTlsEnabledByParameter() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServerTls(false);

        // Create Runtime parameters for enabling TLS
        final CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .layoutServers(Arrays.asList(NodeLocator.parseString(singleNodeEndpoint)))
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .systemDownHandler(getShutdownHandler());

        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT, paramsBuilder);

        // Create CorfuTable
        PersistentCorfuTable<String, Object> testTable = createCorfuTable(runtime, "volbeat");

        // CorfuTable stats before usage
        final int initialSize = testTable.size();

        // Put key values in CorfuTable
        final int count = 100;
        final int entrySize = 1000;
        for (int i = 0; i < count; i++) {
            testTable.insert(String.valueOf(i), new byte[entrySize]);
        }

        // Assert that put operation was successful
        final int sizeAfterPuts = testTable.size();
        assertThat(sizeAfterPuts).isGreaterThanOrEqualTo(initialSize);
        log.info("Initial Table Size: {} - FinalTable Size:{}", initialSize, sizeAfterPuts);

        // Assert that table has correct size (i.e. count) and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that connection between client and server fails when
     * certificate expiry check is enabled on the server and client uses an expired certificate.
     * @throws IOException when File IO Error occurs while loading certs
     * @throws InterruptedException when shutdown retry sleep is interrupted
     */
    @Test
    public void testServerWithEnabledCertExpiry() throws Exception {
        // Overriding with expired runtime keystore
        String runtimePathToKeyStoreExpired = getPropertyAbsolutePath(
                        "runtimePathToKeyStoreExpired");

        // Run a corfu server
        // Enable Tls Mutual Auth to trigger CheckClientTrusted() in ReloadableTrustManager
        Process corfuServer = runSinglePersistentServerTls(true);

        // Create Runtime parameters for enabling TLS
        final CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStoreExpired)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .systemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(getShutdownHandler());

        Assertions.assertThatThrownBy(() -> createRuntime(
                getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort),
                paramsBuilder)).isInstanceOf(UnrecoverableCorfuError.class);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that connection between client and server is successful when
     * certificate expiry check is disabled on the server and client uses an expired certificate.
     * @throws IOException when File IO Error occurs while loading certs
     * @throws InterruptedException when shutdown retry sleep is interrupted
     */
    @Test
    public void testServerWithDisabledCertExpiryCheckAndExpiredCerts() throws Exception {
        // set disableCertExpiryCheckFile
        disableCertExpiryCheckFile = getPropertyAbsolutePath(
                "disableCertExpiryCheckFile");

        // Overriding with expired runtime keystore
        String runtimePathToKeyStoreExpired = getPropertyAbsolutePath(
                "runtimePathToKeyStoreExpired");

        // Run a corfu server
        // Enable Tls Mutual Auth to trigger CheckClientTrusted() in ReloadableTrustManager
        Process corfuServer = runSinglePersistentServerTls(true);

        // Create Runtime parameters for enabling TLS
        final CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStoreExpired)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .disableCertExpiryCheckFile(Paths.get(disableCertExpiryCheckFile))
                .systemDownHandler(getShutdownHandler());

        CorfuRuntime corfuRuntime = createRuntime(
                getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort),
                paramsBuilder);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Test that connection between client and server is successful when
     * certificate expiry check is disabled on the server and client uses a valid certificate.
     * @throws IOException when parsing the properties fails
     * @throws InterruptedException when shutdown retry sleep is interrupted
     */
    @Test
    public void testServerWithDisabledCertExpiryCheckAndValidCerts() throws Exception {
        // set disableCertExpiryCheckFile
        disableCertExpiryCheckFile = getPropertyAbsolutePath(
                "disableCertExpiryCheckFile");


        // Run a corfu server
        // Enable Tls Mutual Auth to trigger CheckClientTrusted() in ReloadableTrustManager
        Process corfuServer = runSinglePersistentServerTls(true);

        // Create Runtime parameters for enabling TLS
        final CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .disableCertExpiryCheckFile(Paths.get(disableCertExpiryCheckFile))
                .systemDownHandler(getShutdownHandler());

        CorfuRuntime corfuRuntime = createRuntime(
                getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort),
                paramsBuilder);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        corfuRuntime.shutdown();
    }

    /**
     * Test that FileWatcher is started in {@link CorfuRuntime#connect()} method and stopped  after
     * it the runtime is shut down.
     * @throws IOException when File IO Error occurs while loading certs
     */
    @Test
    public void testRuntimeFileWatcherLifeCycleWithServer() throws Exception {
        // Run a corfu server
        // Enable Tls Mutual Auth to trigger CheckClientTrusted() in ReloadableTrustManager
        Process corfuServer = runSinglePersistentServerTls(true);

        // Create Runtime parameters for enabling TLS, without calling connect() yet
        final CorfuRuntimeParameters rtParams = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .systemDownHandler(getShutdownHandler())
                .maxMvoCacheEntries(DEFAULT_MVO_CACHE_SIZE)
                .cacheDisabled(true)
                .build();

        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(rtParams)
                    .parseConfigurationString(
                            getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort)
                    );

        // Before Runtime connects FileWatcher should not have been initialized
        assertThat(corfuRuntime.getSslCertWatcher()).isEmpty();

        corfuRuntime.connect();

        // After Runtime connects FileWatcher should have been initialized
        Optional<FileWatcher> sslCertWatcher = corfuRuntime.getSslCertWatcher();
        assertThat(sslCertWatcher).isNotEmpty();
        assertThat(sslCertWatcher.get().getIsStopped()).isFalse();
        assertThat(sslCertWatcher.get().getIsRegistered()).isTrue();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        corfuRuntime.shutdown();

        // After Runtime is shut down FileWatcher should have been stopped
        assertThat(sslCertWatcher).isNotEmpty();
        assertThat(sslCertWatcher.get().getIsStopped()).isTrue();
        assertThat(sslCertWatcher.get().getIsRegistered()).isFalse();
    }
    /**
     * Test that connection scenarios between client and server during certificate replacement workflows
     * - Server is started with default certs
     * - Certificate replacement is done on the server (this triggers sever channel restart)
     * - the old server channel should no longer be active
     * - if the new certs are good, then clients should auto reconnect on server channel restart
     * - if they are bad, then clients should keep fail and retying until the limit is reached,
     *   and then trigger shutdownHandlers()
     *
     * @throws IOException when parsing the properties fails
     * @throws InterruptedException when shutdown retry sleep is interrupted
     */
    @Test
    public void testServerCertReplacementWithValidAndInvalidCerts() throws Exception {
        // set disableCertExpiryCheckFile
        disableCertExpiryCheckFile = getPropertyAbsolutePath(
                "disableCertExpiryCheckFile");

        // Run a corfu server with default File Watcher settings
        Process corfuServer = runSinglePersistentServerTls(true);

        // Create Runtime parameters for enabling TLS
        final CorfuRuntimeParametersBuilder paramsBuilder = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .disableCertExpiryCheckFile(Paths.get(disableCertExpiryCheckFile))
                .disableFileWatcher(disableFileWatcher)
                .systemDownHandler(getShutdownHandler());

        CorfuRuntime corfuRuntime = createRuntime(
                getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort),
                paramsBuilder);

        assertThat(corfuServer.isAlive()).isTrue();

        // Wait for some time and check that client connects to the server with an initial channel
        Channel initialChannel = null;
        for (int i = 0; i < MAX_RETRY_LIMIT; i++) {
            initialChannel = ((NettyClientRouter)corfuRuntime.getRouter(getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort))).getChannel();
            if (initialChannel.isActive()){
                break;
            }
            TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECONDS);
        }
        assertThat(initialChannel).isNotNull();
        assertThat(initialChannel.isActive()).isTrue();

        // Replace valid certs with invalid ones (Doing MOVE to simulate the client's behavior)
        Files.copy(serverInvalidKeyStorePath, serverInvalidKeyStorePathTemp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(serverInvalidKeyStorePathTemp, serverKeyStoreFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        // Wait for some time and check that client never connects with the server
        for (int i = 0; i < MIN_RETRY_LIMIT; i++) {
            if (!initialChannel.isActive()){
                break;
            }
            TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECONDS);
        }
        assertThat(initialChannel.isActive()).isFalse();

        // Restore back to valid certs
        Files.copy(serverKeyStoreFilePathBackup, serverKeyStoreFilePathTemp, StandardCopyOption.REPLACE_EXISTING);
        Files.move(serverKeyStoreFilePathTemp, serverKeyStoreFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        // Wait for some time and check that client connects to the server with a new channel
        Channel newChannel = null;
        for (int i = 0; i < MAX_RETRY_LIMIT; i++) {
            newChannel = ((NettyClientRouter)corfuRuntime.getRouter(getVersionFormattedEndpointURL(corfuSingleNodeHost, corfuSingleNodePort))).getChannel();
            if (newChannel.isActive()){
                break;
            }
            TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECONDS);
        }
        assertThat(newChannel).isNotNull();
        assertThat(newChannel.isActive()).isTrue();
        assertThat(newChannel.equals(initialChannel)).isFalse();

        // initialChannel should never become active again as it is already closed
        assertThat(initialChannel.isActive()).isFalse();

        // runtime should still be active
        assertThat(corfuRuntime.isShutdown()).isFalse();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        corfuRuntime.shutdown();
    }

    private Runnable getShutdownHandler() {
        return () -> {
            log.error("Unable to connect to Corfu Server!");
            Path testDir = Paths.get(CORFU_LOG_PATH);
            Path buildDir = Paths.get("target/logs", testDir.getFileName().toString());
            Path corfuLogs = testDir.resolve("localhost_9000_consolelog");

            log.info("Save server logs into: {}", buildDir);
            try {
                Files.createDirectories(buildDir);
                Files.copy(corfuLogs, buildDir.resolve("corfu.log"), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            throw new UnreachableClusterException("Unable to connect to Corfu Server!");
        };
    }
}
