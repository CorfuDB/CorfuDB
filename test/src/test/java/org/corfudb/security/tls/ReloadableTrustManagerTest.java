package org.corfudb.security.tls;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.Test;
import static org.junit.Assert.fail;

public class ReloadableTrustManagerTest {
    private String PASSWORD_FILE = "src/test/resources/security/reload/password";
    private String CLIENT_CERT = "src/test/resources/security/reload/client.cert";
    private String SERVER_CERT = "src/test/resources/security/reload/server.cert";

    @Test
    public void testServerCheckClient() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(
                "src/test/resources/security/reload/server_trust_with_client.jks", PASSWORD_FILE);

        X509Certificate cert = getCertificate(CLIENT_CERT);

        manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
    }

    @Test
    public void testServerCheckClientFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(
                "src/test/resources/security/reload/server_trust_no_client.jks", PASSWORD_FILE);

        X509Certificate cert = getCertificate(CLIENT_CERT);

        try {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
            fail();
        } catch (CertificateException e) {

        }
    }

    @Test
    public void testClientCheckServer() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(
                "src/test/resources/security/reload/client_trust_with_server.jks", PASSWORD_FILE);

        X509Certificate cert = getCertificate(SERVER_CERT);

        manager.checkServerTrusted(new X509Certificate[]{cert}, "RSA");
    }

    @Test
    public void testClientCheckServerFail() throws Exception {
        ReloadableTrustManager manager = new ReloadableTrustManager(
                "src/test/resources/security/reload/client_trust_no_server.jks", PASSWORD_FILE);

        X509Certificate cert = getCertificate(SERVER_CERT);

        try {
            manager.checkClientTrusted(new X509Certificate[]{cert}, "RSA");
            fail();
        } catch (CertificateException e) {

        }
    }

    private X509Certificate getCertificate(String certFile) throws Exception {
        String clientCert = (new String(Files.readAllBytes(Paths.get(certFile)))).trim();
        byte [] decoded = Base64.decode(clientCert);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(decoded));
    }
}