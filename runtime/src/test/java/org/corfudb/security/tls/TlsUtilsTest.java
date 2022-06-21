package org.corfudb.security.tls;

import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_WITH_SERVER;
import static org.corfudb.security.tls.TlsTestContext.FAKE_LOCATION_AND_PASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class TlsUtilsTest {

    @Test
    public void testGetPassword() {
        String password = TlsUtils.getCertStorePassword(CLIENT_TRUST_WITH_SERVER.getPasswordFile()).join();
        assertEquals("password", password);
    }

    @Test
    public void testBadPasswordFile() {
        Path fakeLocation = Paths.get("definitely fake location");
        CompletableFuture<String> async = TlsUtils.getCertStorePassword(fakeLocation);
        checkTaskError(async, ex -> {
            assertEquals(TlsUtils.PASSWORD_FILE_NOT_FOUND_ERROR, ex.getMessage());
        });
    }

    @Test
    public void testOpenKeyStore() throws Exception {
        KeyStore keyStore = TlsUtils.openCertStore(CLIENT_TRUST_WITH_SERVER).join();
        assertEquals(2, keyStore.size());
    }

    @Test
    public void testOpenKeyStoreBadPassword() {
        String expectedErrMsg = "Keystore was tampered with, or password was incorrect";

        CompletableFuture<KeyStore> async = TlsUtils.openCertStore(TlsTestContext.FAKE_PASS);
        checkTaskError(async, ex -> {
            assertEquals(expectedErrMsg, ex.getCause().getMessage());
        });
    }

    @Test
    public void testOpenKeyStoreBadLocation() {
        CompletableFuture<KeyStore> async = TlsUtils.openCertStore(FAKE_LOCATION_AND_PASS);
        String expectedErrorMessage = "Key store file {definitely fake location} doesn't exist.";
        checkTaskError(async, (IllegalStateException ex) -> {
            assertEquals(expectedErrorMessage, ex.getMessage());
        });
    }

    @Test
    public void testCertExpiryCheckDisabled(){
        TrustStoreConfig trustStore = TlsTestContext.SERVER_TRUST_WITH_CLIENT;
        assertFalse(trustStore.isCertExpiryCheckDisabled());
    }

    private <T> void checkTaskError(CompletableFuture<T> async, Consumer<IllegalStateException> errHandler) {
        try {
            async.join();
            fail("Must throw exception");
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof IllegalStateException) {
                IllegalStateException error = (IllegalStateException) ex.getCause();
                errHandler.accept(error);
            } else {
                fail("Unexpected exception", ex.getCause());
            }
        }
    }
}
