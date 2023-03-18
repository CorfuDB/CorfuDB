package org.corfudb.security.tls;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

/**
 * This trust manager reloads the trust store whenever a checkClientTrusted
 * or checkServerTrusted is called.
 */
@Slf4j
public class ReloadableTrustManager implements X509TrustManager {
    private final TrustStoreConfig trustStoreConfig;
    private X509TrustManager trustManager;

    /**
     * Constructor.
     *
     * @param trustStoreConfig Location of trust store.
     */
    public ReloadableTrustManager(TrustStoreConfig trustStoreConfig) throws SSLException {
        this.trustStoreConfig = trustStoreConfig;
        reloadTrustStore();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        reloadTrustStoreWrapper();
        checkValidity(chain);
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        reloadTrustStoreWrapper();
        checkValidity(chain);
        trustManager.checkServerTrusted(chain, authType);
    }

    private void checkValidity(X509Certificate[] chain) throws CertificateExpiredException, CertificateNotYetValidException {
        if (isCertExpiryCheckEnabled()) {
            log.trace("checkValidity: CertExpiryCheck is Enabled.");
            for (X509Certificate cert : chain) {
                cert.checkValidity();
            }
        } else {
            logCertExpiryCheck();
        }
    }

    private boolean isCertExpiryCheckEnabled() {
        return trustStoreConfig.isCertExpiryCheckEnabled();
    }

    private void logCertExpiryCheck() {
        log.info(
                "Certificate expiry check has been disabled with: {}",
                trustStoreConfig.getDisableCertExpiryCheckFile()
        );
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    /**
     * Just a wrapper due to IDE pointing out duplicate code.
     *
     * @throws CertificateException Wrapper for any exception from reloading the trust store.
     */
    private void reloadTrustStoreWrapper() throws CertificateException {
        try {
            reloadTrustStore();
        } catch (SSLException e) {
            String message = "Unable to reload trust store " + trustStoreConfig.getTrustStoreFile() + ".";
            throw new CertificateException(message, e);
        }
    }

    /**
     * Reload the trust manager.
     *
     * @throws SSLException Thrown when there's an issue with loading the trust store.
     */
    private void reloadTrustStore() throws SSLException {
        KeyStore trustStore = TlsUtils.openCertStore(trustStoreConfig);

        TrustManagerFactory tmf;
        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "No support for TrustManagerFactory default algorithm "
                    + TrustManagerFactory.getDefaultAlgorithm() + ".";
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Unable to load trust store " + trustStoreConfig.getTrustStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        }

        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                trustManager = (X509TrustManager) tm;
                return;
            }
        }

        throw new SSLException("No X509TrustManager in TrustManagerFactory.");
    }
}
