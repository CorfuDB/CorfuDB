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
import java.security.NoSuchProviderException;
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
    
    // TrustManagerFactory algorithm constants
    private static final String ALGORITHM_PKIX = "PKIX";
    private static final String ALGORITHM_SUNX509 = "SunX509";
    
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
        TlsUtils.KeyStoreResult trustStoreResult = TlsUtils.openCertStoreWithType(trustStoreConfig);
        KeyStore trustStore = trustStoreResult.getKeyStore();

        TrustManagerFactory tmf = createTrustManagerFactory(trustStoreResult);
        
        try {
            tmf.init(trustStore);
        } catch (KeyStoreException e) {
            String errorMessage = "Unable to load trust store " + trustStoreConfig.getTrustStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        }

        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                trustManager = (X509TrustManager) tm;
                log.info("Successfully reloaded trust store (type: {}, provider: {}).",
                        trustStoreResult.getKeyStoreType(), trustStoreResult.getProvider());
                return;
            }
        }

        throw new SSLException("No X509TrustManager in TrustManagerFactory.");
    }

    /**
     * Creates a TrustManagerFactory appropriate for the trust store type.
     * For BCFKS trust stores with BCFIPS provider, uses PKIX algorithm with BCJSSE provider.
     * For standard JKS/PKCS12 trust stores, tries PKIX first, then falls back to SunX509.
     *
     * @param trustStoreResult the trust store result containing type information
     * @return TrustManagerFactory configured for the trust store type
     * @throws SSLException if no suitable TrustManagerFactory can be created
     */
    private TrustManagerFactory createTrustManagerFactory(TlsUtils.KeyStoreResult trustStoreResult) throws SSLException {
        if (trustStoreResult.isBcFips()) {
            // For BCFIPS, use PKIX algorithm with BCJSSE provider
            return createBcFipsTrustManagerFactory();
        } else {
            // For standard JDK trust stores (JKS, PKCS12), try PKIX first, then SunX509
            return createStandardTrustManagerFactory();
        }
    }

    /**
     * Creates a TrustManagerFactory for BCFIPS trust stores.
     * Uses PKIX algorithm with BCJSSE provider if available.
     */
    private TrustManagerFactory createBcFipsTrustManagerFactory() throws SSLException {
        try {
            // Try PKIX with BCJSSE provider for BCFIPS
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM_PKIX, "BCJSSE");
            log.info("Created TrustManagerFactory with algorithm {} and provider BCJSSE", ALGORITHM_PKIX);
            return tmf;
        } catch (NoSuchProviderException e) {
            log.warn("BCJSSE provider not available, trying default PKIX: {}", e.getMessage());
            // Fall back to default PKIX
            try {
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM_PKIX);
                log.info("Created TrustManagerFactory with algorithm {} (default provider)", ALGORITHM_PKIX);
                return tmf;
            } catch (NoSuchAlgorithmException e2) {
                String errorMessage = "Cannot create TrustManagerFactory with algorithm " + ALGORITHM_PKIX;
                throw new SSLException(errorMessage, e2);
            }
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Cannot create TrustManagerFactory with algorithm " + ALGORITHM_PKIX;
            throw new SSLException(errorMessage, e);
        }
    }

    /**
     * Creates a TrustManagerFactory for standard JDK trust stores (JKS, PKCS12).
     * Tries PKIX algorithm first for broader compatibility, then falls back to SunX509.
     */
    private TrustManagerFactory createStandardTrustManagerFactory() throws SSLException {
        // Try PKIX first - it's the standard algorithm that works with most trust stores
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM_PKIX);
            log.info("Created TrustManagerFactory with algorithm {}", ALGORITHM_PKIX);
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            log.info("PKIX algorithm not available, trying SunX509: {}", e.getMessage());
        }

        // Fall back to SunX509 for traditional JDK environments
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(ALGORITHM_SUNX509);
            log.info("Created TrustManagerFactory with algorithm {}", ALGORITHM_SUNX509);
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            log.info("SunX509 algorithm not available, trying default: {}", e.getMessage());
        }

        // Last resort: use default algorithm
        try {
            String defaultAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(defaultAlgorithm);
            log.info("Created TrustManagerFactory with default algorithm {}", defaultAlgorithm);
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Cannot create TrustManagerFactory with any available algorithm";
            throw new SSLException(errorMessage, e);
        }
    }
}
