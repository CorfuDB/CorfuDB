package org.corfudb.security.tls;

import org.apache.xerces.impl.dv.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.corfudb.security.tls.TlsTestContext.CLIENT_CERT;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_NO_SERVER;
import static org.corfudb.security.tls.TlsTestContext.CLIENT_TRUST_WITH_SERVER;
import static org.corfudb.security.tls.TlsTestContext.SERVER_CERT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_NO_CLIENT;
import static org.corfudb.security.tls.TlsTestContext.SERVER_TRUST_WITH_CLIENT;

public class ReloadableTrustManagerTest {

    @Test
    public void testServerCheckClient() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(SERVER_TRUST_WITH_CLIENT);
        X509Certificate cert = getCertificate(CLIENT_CERT);
        manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
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
    public void testClientCheckServer() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(CLIENT_TRUST_WITH_SERVER);
        X509Certificate cert = getCertificate(SERVER_CERT);
        manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
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
}