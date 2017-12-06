package org.corfudb.security.tls;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import lombok.extern.slf4j.Slf4j;

/**
 * This trust manager reloads the trust store whenever a checkClientTrusted
 * or checkServerTrusted is called.
 *
 * Created by zjohnny on 9/18/17.
 */
@Slf4j
public class ReloadableTrustManager implements X509TrustManager {
    private String trustStorePath, trustPasswordPath;
    private X509TrustManager trustManager;

    /**
     * Constructor.
     *
     * @param trustStorePath
     *          Location of trust store.
     * @param trustPasswordPath
     *          Location of trust store password.
     * @throws SSLException
     *          Thrown when there's an issue with loading the trust store.
     */
    public ReloadableTrustManager(String trustStorePath, String trustPasswordPath) throws SSLException {
        this.trustStorePath = trustStorePath;
        this.trustPasswordPath = trustPasswordPath;
        reloadTrustStore();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        reloadTrustStoreWrapper();
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        reloadTrustStoreWrapper();
        trustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    /**
     * Just a wrapper due to IDE pointing out duplicate code.
     *
     * @throws CertificateException
     *          Wrapper for any exception from reloading the trust store.
     */
    private void reloadTrustStoreWrapper() throws CertificateException {
        try {
            reloadTrustStore();
        } catch (SSLException e) {
            String message = "Unable to reload trust store " + trustStorePath + ".";
            log.error(message, e);
            throw new CertificateException(message, e);
        }
    }

    /**
     * Reload the trust manager.
     *
     * @throws SSLException
     *          Thrown when there's an issue with loading the trust store.
     */
    private void reloadTrustStore() throws SSLException {
        String trustPassword = TlsUtils.getKeyStorePassword(trustPasswordPath);
        KeyStore trustStore = TlsUtils.openKeyStore(trustStorePath, trustPassword);

        TrustManagerFactory tmf;
        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "No support for TrustManagerFactory default algorithm "
                    + TrustManagerFactory.getDefaultAlgorithm() + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Unable to load trust store " + trustStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        }

        for (TrustManager tm: tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                trustManager = (X509TrustManager)tm;
                return;
            }
        }

        throw new SSLException("No X509TrustManager in TrustManagerFactory.");
    }
}
