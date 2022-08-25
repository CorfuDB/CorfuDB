package org.corfudb.security.tls;

import org.apache.xerces.impl.dv.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.corfudb.security.tls.TlsTestContext.CLIENT_CERT;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_NO_SERVER;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_WITH_SERVER;
import static org.corfudb.security.tls.TlsTestContext.SERVER_CERT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_NO_CLIENT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_WITH_CLIENT;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReloadableTrustManagerTest {

    @Test
    public void testServerCheckClient() throws Exception {
        withDisabledCheckExpiry(() -> {
            ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_WITH_CLIENT);
            X509Certificate cert = getCertificate(CLIENT_CERT);
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testServerCheckClientExpiration() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_WITH_CLIENT);
        X509Certificate cert = getCertificate(CLIENT_CERT);
        assertThrows(CertificateExpiredException.class, () -> {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testServerCheckClientFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_NO_CLIENT);

        X509Certificate cert = getCertificate(CLIENT_CERT);

        try {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
            Assertions.fail();
        } catch (CertificateException e) {
            //ignore
        }
    }

    @Test
    public void testClientCheckServerExpiration() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_WITH_SERVER);
        X509Certificate cert = getCertificate(SERVER_CERT);
        assertThrows(CertificateExpiredException.class, () -> {
            manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testClientCheckServer() throws Exception {
        withDisabledCheckExpiry(() -> {
            ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_WITH_SERVER);
            X509Certificate cert = getCertificate(SERVER_CERT);
            manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testClientCheckServerFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_NO_SERVER);

        X509Certificate cert = getCertificate(SERVER_CERT);

        try {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
            Assertions.fail();
        } catch (CertificateException e) {
            //ignore
        }
    }

    private X509Certificate getCertificate(Path certFile) throws Exception {
        String clientCert = new String(Files.readAllBytes(certFile));
        clientCert = clientCert.trim();
        byte[] decoded = Base64.decode(clientCert);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(decoded));
    }

    private void withDisabledCheckExpiry(ThrowableAction action) throws Exception {
        File certExpiryCheck = SERVER_TRUST_WITH_CLIENT.getDisableCertExpiryCheckFile().toFile();
        try {
            certExpiryCheck.createNewFile();
            action.run();
        } finally {
            certExpiryCheck.deleteOnExit();
            certExpiryCheck.delete();
        }
    }

    private interface ThrowableAction {
        void run() throws Exception;
    }
}
