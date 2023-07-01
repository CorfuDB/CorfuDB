package org.corfudb.security.tls;

import org.apache.xerces.impl.dv.util.Base64;
import org.corfudb.security.tls.TlsTestContext.ValidCerts;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.corfudb.security.tls.TlsTestContext.CLIENT_CERT;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_CERT_ECDSA_EXPIRED;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_ECDSA_EXPIRED_NO_SERVER;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_NO_SERVER;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_WITH_SERVER;
import static org.corfudb.security.tls.TlsTestContext.SERVER_CERT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_NO_CLIENT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_WITH_CLIENT;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReloadableTrustManagerTest {

    @Test
    public void testServerCheckClientRSA() throws Exception {
        withDisabledCheckExpiry(() -> {
            ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_WITH_CLIENT);
            X509Certificate cert = getCertificate(CLIENT_CERT, true);
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    /**
     * Test that server can validate the expired client certificate of type ECDSA
     * when certificate expiry check is disabled.
     *
     * @throws Exception any exceptions caught while validating the certificate
     */
    @Test
    public void testServerCheckClientEC() throws Exception {
        withDisabledCheckExpiry(() -> {
            ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_ECDSA_EXPIRED_NO_SERVER);
            X509Certificate cert = getCertificate(CLIENT_CERT_ECDSA_EXPIRED, false);
            manager.checkClientTrusted(new X509Certificate[]{cert}, "ECDHE_RSA");
        });
    }

    @Test
    public void testServerCheckClientForValidCerts() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(ValidCerts.TRUST_STORE_CONFIG);
        X509Certificate cert = getCertificate(ValidCerts.RUNTIME_CERT, false);
        manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
    }

    @Test
    public void testServerCheckClientExpiration() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_WITH_CLIENT);
        X509Certificate cert = getCertificate(CLIENT_CERT, true);
        assertThrows(CertificateExpiredException.class, () -> {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testServerCheckClientFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_NO_CLIENT);

        X509Certificate cert = getCertificate(CLIENT_CERT, true);

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
        X509Certificate cert = getCertificate(SERVER_CERT, true);
        assertThrows(CertificateExpiredException.class, () -> {
            manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testClientCheckServer() throws Exception {
        withDisabledCheckExpiry(() -> {
            ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_WITH_SERVER);
            X509Certificate cert = getCertificate(SERVER_CERT, true);
            manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
        });
    }

    @Test
    public void testClientCheckServerFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_NO_SERVER);

        X509Certificate cert = getCertificate(SERVER_CERT, true);

        try {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
            Assertions.fail();
        } catch (CertificateException e) {
            //ignore
        }
    }

    private X509Certificate getCertificate(Path certFile, boolean base64encoded) throws Exception {
        String clientCert = new String(Files.readAllBytes(certFile));
        clientCert = clientCert.trim();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        byte[] decoded;
        if (base64encoded) {
            decoded = Base64.decode(clientCert);
        } else {
            decoded = clientCert.getBytes(Charset.defaultCharset());
        }
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
