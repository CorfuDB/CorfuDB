package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.SMRObject;

import org.corfudb.runtime.collections.CorfuTable;

import org.corfudb.security.tls.ReloadableTrustManager.TrustStoreWatcher;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This test suit exercises the ability to enable TLS on Corfu servers and runtime
 * <p>
 * Created by Sam Behnam on 8/13/18.
 */
@Slf4j
public class SecurityIT extends AbstractIT {
    private static final TypeToken<PersistentCorfuTable<String, Object>> TYPE_TOKEN
            = new TypeToken<PersistentCorfuTable<String, Object>>() {};

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

    /**
     * This is a helper method that loads the properties required for creating corfu servers before
     * each test by converting them from the values provided in CorfuDB.properties. Note that it
     * will throw {@link NumberFormatException} if the numerical properties provided in CorfuDB.properties
     * are not well-formed. If the CorfuDB.properties has invalid or non existent values for the keystore or
     * truststore, it will lead to throwing {@link IllegalArgumentException}.
     */
    @BeforeEach
    public void loadProperties() {
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
     * @throws Exception error
     */
    @Test
    public void testServerRuntimeTlsEnabledMethod() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServerTls();

        // Start a Corfu runtime
        CorfuRuntimeParameters runtimeParameters = CorfuRuntimeParameters
                .builder()
                .layoutServers(Collections.singletonList(NodeLocator.parseString(singleNodeEndpoint)))
                .tlsEnabled(tlsEnabled)
                .keyStore(runtimePathToKeyStore)
                .ksPasswordFile(runtimePathToKeyStorePassword)
                .trustStore(runtimePathToTrustStore)
                .tsPasswordFile(runtimePathToTrustStorePassword)
                .cacheDisabled(true)
                .systemDownHandler(() -> fail("Can't connect to corfu server"))
                .build();

        // Connecting to runtime
        runtime = CorfuRuntime
                .fromParameters(runtimeParameters)
                .connect();

        // Create CorfuTable
        PersistentCorfuTable<String, Object> testTable = runtime
                .getObjectsView()
                .build()
                .setTypeToken(TYPE_TOKEN)
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
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

        // Assert that table has correct size (i.e. count) and and server is shutdown
        assertThat(testTable.size()).isEqualTo(count);
        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Testing that configuring incorrect TLS parameters will lead to throwing
     * {@link IllegalStateException} exception.
     */
    @Test
    public void testInvalidKeyStore() {
        assertThrows(IllegalStateException.class, () -> {
            SslContextConstructor.constructSslContext(
                    false,
                    KeyStoreConfig.from(runtimePathToTrustStore, runtimePathToKeyStorePassword),
                    TrustStoreConfig.from(
                            runtimePathToTrustStore,
                            runtimePathToTrustStorePassword,
                            TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE
                    )
            );
        });
    }

    @Test
    public void testInvalidTrustStore() {
        TrustStoreConfig invalidTrustStore = TrustStoreConfig.from(
                runtimePathToKeyStore,
                runtimePathToTrustStorePassword,
                TrustStoreConfig.DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE
        );

        assertThrows(IllegalStateException.class, () -> new TrustStoreWatcher(invalidTrustStore));
    }
}
