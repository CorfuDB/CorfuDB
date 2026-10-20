package org.corfudb.security.tls;

import org.corfudb.security.tls.TlsTestContext.ValidCerts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLException;
import javax.net.ssl.KeyManagerFactory;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.OutputStream;
import java.security.Security;
import java.security.KeyStore;

import static org.corfudb.security.tls.TlsTestContext.FAKE_LOCATION_AND_PASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class TlsUtilsTest {

    @TempDir
    Path temporaryDirectory;

    @ParameterizedTest
    @ValueSource(strings = {"JKS", "PKCS12"})
    public void testDetectExistingFormatDespiteDefault(String type) throws Exception {
        String password = "test123";
        Path storeFile = temporaryDirectory.resolve("store.jks");
        Path passwordFile = temporaryDirectory.resolve("password");
        Files.writeString(passwordFile, password);
        KeyStore original = KeyStore.getInstance(type);
        original.load(null, password.toCharArray());
        KeyStore certificates = TlsUtils.openCertStore(ValidCerts.TRUST_STORE_CONFIG);
        String alias = certificates.aliases().nextElement();
        original.setCertificateEntry("trusted", certificates.getCertificate(alias));
        KeyStore keys = TlsUtils.openCertStore(new TlsUtils.CertStoreConfig.KeyStoreConfig(
                ValidCerts.CERT_DIR.resolve("runtime_ecdsa.keystore.jks"), ValidCerts.PASSWORD_FILE));
        String keyAlias = keys.aliases().nextElement();
        original.setKeyEntry("private", keys.getKey(keyAlias, password.toCharArray()),
                password.toCharArray(), keys.getCertificateChain(keyAlias));
        try (OutputStream output = Files.newOutputStream(storeFile)) {
            original.store(output, password.toCharArray());
        }
        byte[] before = Files.readAllBytes(storeFile);
        String previousDefault = Security.getProperty("keystore.type");
        String previousCompatibility = Security.getProperty("keystore.type.compat");
        try {
            Security.setProperty("keystore.type", type.equals("JKS") ? "PKCS12" : "JKS");
            Security.setProperty("keystore.type.compat", "false");
            KeyStore loaded = TlsUtils.openCertStore(
                    new TlsUtils.CertStoreConfig.KeyStoreConfig(storeFile, passwordFile));
            assertEquals(type, loaded.getType().toUpperCase(java.util.Locale.ROOT));
            assertEquals(original.getCertificate("trusted"), loaded.getCertificate("trusted"));
            assertEquals(original.getKey("private", password.toCharArray()),
                    loaded.getKey("private", password.toCharArray()));
            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(loaded, password.toCharArray());
            assertTrue(factory.getKeyManagers().length > 0);
            KeyStore trust = TlsUtils.openCertStore(new TlsUtils.CertStoreConfig.TrustStoreConfig(
                    storeFile, passwordFile, temporaryDirectory.resolve("disable-expiry")));
            assertEquals(original.getCertificate("trusted"), trust.getCertificate("trusted"));
            assertArrayEquals(before, Files.readAllBytes(storeFile));
        } finally {
            Security.setProperty("keystore.type", previousDefault);
            Security.setProperty("keystore.type.compat", previousCompatibility == null ? "true" : previousCompatibility);
        }
    }

    @Test
    public void testRejectInvalidStoreAndDirectory() throws Exception {
        Path password = temporaryDirectory.resolve("password");
        Files.writeString(password, "test123");
        Path invalid = temporaryDirectory.resolve("invalid.jks");
        Files.writeString(invalid, "not a keystore");
        assertThrows(SSLException.class, () -> TlsUtils.openCertStore(
                new TlsUtils.CertStoreConfig.KeyStoreConfig(invalid, password)));
        assertThrows(SSLException.class, () -> TlsUtils.openCertStore(
                new TlsUtils.CertStoreConfig.KeyStoreConfig(temporaryDirectory, password)));
    }

    @Test
    public void testGetPassword() throws Exception {
        String password = TlsUtils.getKeyStorePassword(ValidCerts.PASSWORD_FILE);
        assertEquals("test123", password);
    }

    @Test
    public void testBadPasswordFile() {
        try {
            TlsUtils.getKeyStorePassword(Paths.get("definitely fake location"));
            fail("Must throw SSLException");
        } catch (SSLException e) {
            assertEquals(TlsUtils.PASSWORD_FILE_NOT_FOUND_ERROR, e.getMessage());
        }
    }

    @Test
    public void testOpenKeyStore() throws Exception {
        KeyStore keyStore = TlsUtils.openCertStore(ValidCerts.TRUST_STORE_CONFIG);
        assertEquals(2, keyStore.size());
    }

    @Test
    public void testOpenKeyStoreBadPassword() {
        try {
            TlsUtils.openCertStore(TlsTestContext.FAKE_PASS);
            fail("Must throw SSLException");
        } catch (SSLException e) {
            assertEquals(
                    "Keystore was tampered with, or password was incorrect",
                    e.getCause().getMessage()
            );
        }
    }

    @Test
    public void testOpenKeyStoreBadLocation() {
        try {
            TlsUtils.openCertStore(FAKE_LOCATION_AND_PASS);
            fail("Must throw SSLException");
        } catch (SSLException e) {
            assertTrue(e.getMessage().endsWith("doesn't exist."));
        }
    }
}
