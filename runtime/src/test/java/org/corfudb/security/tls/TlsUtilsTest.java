package org.corfudb.security.tls;

import org.corfudb.security.tls.TlsTestContext.ValidCerts;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;
import java.nio.file.Paths;
import java.security.KeyStore;

import static org.corfudb.security.tls.TlsTestContext.FAKE_LOCATION_AND_PASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class TlsUtilsTest {

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
