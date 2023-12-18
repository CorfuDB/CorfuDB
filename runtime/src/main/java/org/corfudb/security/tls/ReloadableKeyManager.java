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
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

@Slf4j
public class ReloadableKeyManager implements X509KeyManager {

    private final TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig;

    private X509KeyManager keyManager;

    private long keysLastModifiedTime;

    public ReloadableKeyManager(TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig) {
        this.keyStoreConfig = keyStoreConfig;
        this.keysLastModifiedTime = 0;
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
            reloadKeyStore();
        } catch (SSLException e) {
            String message = "Unable to reload key store " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new RuntimeException(message, e);
        }
    }

    private void reloadKeyStore() throws SSLException {
        if (!hasKeysChanged()) {
            log.debug("Key store certs hasn't changed. Skip reloading.");
            return;
        }

        log.info("Reloading key store from {}", keyStoreConfig.getKeyStoreFile());

        KeyStore keyStore = TlsUtils.openCertStore(keyStoreConfig);
        String keyStorePassword = TlsUtils.getKeyStorePassword(keyStoreConfig.getPasswordFile());

        KeyManagerFactory kmf;
        try {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyStorePassword.toCharArray());
        } catch (UnrecoverableKeyException e) {
            String errorMessage = "Unrecoverable key in key store " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Can not create key manager factory with default algorithm "
                    + KeyManagerFactory.getDefaultAlgorithm() + ".";
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Can not initialize key manager factory from " + keyStoreConfig.getKeyStoreFile() + ".";
            throw new SSLException(errorMessage, e);
        }

        for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509KeyManager) {
                keyManager = (X509KeyManager) km;
                return;
            }
        }

        throw new SSLException("No X509KeyManager in KeyManagerFactory.");
    }
}
