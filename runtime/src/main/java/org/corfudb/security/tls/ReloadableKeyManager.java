package org.corfudb.security.tls;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509KeyManager;
import java.net.Socket;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

@Slf4j
public class ReloadableKeyManager implements X509KeyManager {

    // KeyManagerFactory algorithm constants
    private static final String ALGORITHM_PKIX = "PKIX";
    private static final String ALGORITHM_SUNX509 = "SunX509";

    private final TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig;

    private X509KeyManager keyManager;

    private long keysLastModifiedTime;

    private boolean lastReloadSucceeded;

    public ReloadableKeyManager(TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig) {
        this.keyStoreConfig = keyStoreConfig;
        this.keysLastModifiedTime = 0;
        this.lastReloadSucceeded = false;
        reloadKeyStoreWrapper();
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        reloadKeyStoreWrapper();
        return keyManager.getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        reloadKeyStoreWrapper();
        return keyManager.chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        reloadKeyStoreWrapper();
        return keyManager.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        reloadKeyStoreWrapper();
        return keyManager.chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        reloadKeyStoreWrapper();
        return keyManager.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        reloadKeyStoreWrapper();
        return keyManager.getPrivateKey(alias);
    }

    private boolean hasKeysChanged() {
        long lastModified = keyStoreConfig.getKeyStoreFile().toFile().lastModified();
        if (lastModified != keysLastModifiedTime) {
            keysLastModifiedTime = lastModified;
            return true;
        }
        return false;
    }

    private void reloadKeyStoreWrapper() {
        try {
            if (!hasKeysChanged() && lastReloadSucceeded) {
                log.info("Key store file hasn't changed since last successful reload at {}. Skip reloading.",
                        keysLastModifiedTime);
                return;
            }
            reloadKeyStore();
        } catch (SSLException e) {
            String message = "Unable to reload key store " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new IllegalStateException(message, e);
        }
    }

    private void reloadKeyStore() throws SSLException {
        log.info("Reloading key store from {}. lastReloadSucceeded: {}, keysLastModifiedTime: {}.",
                keyStoreConfig.getKeyStoreFile(), lastReloadSucceeded, keysLastModifiedTime);

        lastReloadSucceeded = false;
        
        // Use openCertStoreWithType to get keystore type information
        TlsUtils.KeyStoreResult keyStoreResult = TlsUtils.openCertStoreWithType(keyStoreConfig);
        KeyStore keyStore = keyStoreResult.getKeyStore();
        String keyStorePassword = TlsUtils.getKeyStorePassword(keyStoreConfig.getPasswordFile());

        KeyManagerFactory kmf = createKeyManagerFactory(keyStoreResult);
        
        try {
            kmf.init(keyStore, keyStorePassword.toCharArray());
        } catch (UnrecoverableKeyException e) {
            String errorMessage = "Unrecoverable key in key store " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Can not initialize key manager factory from " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "KeyManagerFactory algorithm error for " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        }

        for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509KeyManager) {
                keyManager = (X509KeyManager) km;
                lastReloadSucceeded = true;
                log.info("Successfully reloaded keystore (type: {}, provider: {}).", 
                        keyStoreResult.getKeyStoreType(), keyStoreResult.getProvider());
                return;
            }
        }

        throw new SSLException("No X509KeyManager in KeyManagerFactory.");
    }

    /**
     * Creates a KeyManagerFactory appropriate for the keystore type.
     * For BCFKS keystores with BCFIPS provider, uses PKIX algorithm with BCJSSE provider.
     * For standard JKS/PKCS12 keystores, tries PKIX first, then falls back to SunX509.
     *
     * @param keyStoreResult the keystore result containing type information
     * @return KeyManagerFactory configured for the keystore type
     * @throws SSLException if no suitable KeyManagerFactory can be created
     */
    private KeyManagerFactory createKeyManagerFactory(TlsUtils.KeyStoreResult keyStoreResult) throws SSLException {
        if (keyStoreResult.isBcFips()) {
            // For BCFIPS, use PKIX algorithm with BCJSSE provider
            return createBcFipsKeyManagerFactory();
        } else {
            // For standard JDK keystores (JKS, PKCS12), try PKIX first, then SunX509
            return createStandardKeyManagerFactory();
        }
    }

    /**
     * Creates a KeyManagerFactory for BCFIPS keystores.
     * Uses PKIX algorithm with BCJSSE provider if available.
     */
    private KeyManagerFactory createBcFipsKeyManagerFactory() throws SSLException {
        try {
            // Try PKIX with BCJSSE provider for BCFIPS
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM_PKIX, "BCJSSE");
            log.info("Created KeyManagerFactory with algorithm {} and provider BCJSSE", ALGORITHM_PKIX);
            return kmf;
        } catch (NoSuchProviderException e) {
            log.warn("BCJSSE provider not available, trying default PKIX: {}", e.getMessage());
            // Fall back to default PKIX
            try {
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM_PKIX);
                log.info("Created KeyManagerFactory with algorithm {} (default provider)", ALGORITHM_PKIX);
                return kmf;
            } catch (NoSuchAlgorithmException e2) {
                String errorMessage = "Cannot create KeyManagerFactory with algorithm " + ALGORITHM_PKIX;
                throw new SSLException(errorMessage, e2);
            }
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Cannot create KeyManagerFactory with algorithm " + ALGORITHM_PKIX;
            throw new SSLException(errorMessage, e);
        }
    }

    /**
     * Creates a KeyManagerFactory for standard JDK keystores (JKS, PKCS12).
     * Tries PKIX algorithm first for broader compatibility, then falls back to SunX509.
     */
    private KeyManagerFactory createStandardKeyManagerFactory() throws SSLException {
        // Try PKIX first - it's the standard algorithm that works with most keystores
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM_PKIX);
            log.info("Created KeyManagerFactory with algorithm {}", ALGORITHM_PKIX);
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            log.info("PKIX algorithm not available, trying SunX509: {}", e.getMessage());
        }

        // Fall back to SunX509 for traditional JDK environments
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(ALGORITHM_SUNX509);
            log.info("Created KeyManagerFactory with algorithm {}", ALGORITHM_SUNX509);
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            log.info("SunX509 algorithm not available, trying default: {}", e.getMessage());
        }

        // Last resort: use default algorithm
        try {
            String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(defaultAlgorithm);
            log.info("Created KeyManagerFactory with default algorithm {}", defaultAlgorithm);
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Cannot create KeyManagerFactory with any available algorithm";
            throw new SSLException(errorMessage, e);
        }
    }
}

