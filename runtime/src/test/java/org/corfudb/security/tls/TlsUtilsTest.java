package org.corfudb.security.tls;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.security.KeyStore;

import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_WITH_SERVER;
import static org.corfudb.security.tls.TlsTestContext.FAKE_LOCATION_AND_PASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TlsUtilsTest {

    @Test
    public void testGetPassword() {
        String password = TlsUtils.getCertStorePassword(CLIENT_TRUST_WITH_SERVER.getPasswordFile()).join();
        assertEquals("password", password);
    }

    @Test
    public void testBadPasswordFile() {
        try {
            TlsUtils.getCertStorePassword(Paths.get("definitely fake location"));
        } catch (Exception e) {
            assertEquals(TlsUtils.PASSWORD_FILE_NOT_FOUND_ERROR, e.getMessage());
        }
    }

    @Test
    public void testOpenKeyStore() throws Exception {
        KeyStore keyStore = TlsUtils.openCertStore(CLIENT_TRUST_WITH_SERVER).join();
        assertEquals(2, keyStore.size());
    }

    @Test
    public void testOpenKeyStoreBadPassword() {
        try {
            TlsUtils.openCertStore(TlsTestContext.FAKE_PASS);
        } catch (Exception e) {
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
        } catch (Exception e) {
            assertTrue(e.getMessage().endsWith("doesn't exist."));
        }
    }
}
