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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ReloadableKeyManager implements X509KeyManager {

    /**
     * Netty's native OpenSSL engine asks the key manager for a server alias using the public key
     * algorithm names listed in OpenSslKeyMaterialManager (RSA, EC, EC_EC, EC_RSA). The BCJSSE
     * PKIX key manager only recognises the TLS 1.2 key exchange + authentication names, so for
     * example an EC certificate is found with "ECDHE_ECDSA" but not with "EC". Maps the name
     * Netty asks for to the names BCJSSE understands, in the order they are tried.
     */
    private static final Map<String, String[]> SERVER_KEY_TYPE_FALLBACKS;

    static {
        Map<String, String[]> fallbacks = new HashMap<>();
        fallbacks.put("EC", new String[]{"ECDHE_ECDSA"});
        fallbacks.put("EC_EC", new String[]{"ECDH_ECDSA"});
        fallbacks.put("RSA", new String[]{"ECDHE_RSA", "DHE_RSA"});
        fallbacks.put("EC_RSA", new String[]{"ECDH_RSA"});
        fallbacks.put("DSA", new String[]{"DHE_DSA"});
        SERVER_KEY_TYPE_FALLBACKS = Collections.unmodifiableMap(fallbacks);
    }

    private final TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig;

    private X509KeyManager keyManager;

    private long keysLastModifiedTime;

    private boolean lastReloadSucceeded;

    /**
     * Last outcome of each alias lookup, keyed by lookup kind and key type. Alias lookups happen on
     * every TLS handshake, so an outcome is logged at info only when it is new or has changed, and
     * the map is cleared whenever the key store is reloaded so that the first lookup against a new
     * certificate is always logged. Identical repeats are logged at debug.
     */
    private final Map<String, String> lastLoggedOutcomes = new ConcurrentHashMap<>();

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
        // BCJSSE accepts the same key type names Netty uses on the client side (RSA, DSA, EC),
        // so no fallback is needed here. Only log the outcome to trace which alias was picked.
        String alias = keyManager.chooseClientAlias(keyType, issuers, socket);
        logOutcome(lastLoggedOutcomes, "client", Arrays.toString(keyType), String.valueOf(alias),
                "Client alias for key types {}: {}.", Arrays.toString(keyType), alias);
        return alias;
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        reloadKeyStoreWrapper();
        return getServerAliases(lastLoggedOutcomes, keyManager, keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        reloadKeyStoreWrapper();
        return chooseServerAlias(lastLoggedOutcomes, keyManager, keyType, issuers, socket);
    }

    /**
     * Ask the delegate for the requested key type first so that providers which accept it keep
     * their behavior, and only then try the spellings BCJSSE uses for the same key family.
     * Names without a mapping (e.g. TLS 1.3 signature scheme names) are never rewritten.
     */
    static String chooseServerAlias(Map<String, String> outcomes, X509KeyManager delegate, String keyType, Principal[] issuers, Socket socket) {
        String alias = delegate.chooseServerAlias(keyType, issuers, socket);
        if (alias != null) {
            logOutcome(outcomes, "chooseServer", keyType, alias,
                    "Chose server alias {} for key type {}.", alias, keyType);
            return alias;
        }

        String[] fallbacks = fallbacksFor(keyType);
        for (String fallback : fallbacks) {
            alias = delegate.chooseServerAlias(fallback, issuers, socket);
            if (alias != null) {
                logOutcome(outcomes, "chooseServer", keyType, alias + " via " + fallback,
                        "Chose server alias {} for key type {} using fallback key type {}.",
                        alias, keyType, fallback);
                return alias;
            }
        }

        logOutcome(outcomes, "chooseServer", keyType, null,
                "No server alias for key type {}. Fallback key types tried: {}.",
                keyType, Arrays.toString(fallbacks));
        return null;
    }

    /**
     * Same lookup order as {@link #chooseServerAlias(Map, X509KeyManager, String, Principal[], Socket)},
     * so that listing aliases and choosing one agree on the key type names.
     */
    static String[] getServerAliases(Map<String, String> outcomes, X509KeyManager delegate, String keyType, Principal[] issuers) {
        String[] aliases = delegate.getServerAliases(keyType, issuers);
        if (aliases != null) {
            logOutcome(outcomes, "serverAliases", keyType, Arrays.toString(aliases),
                    "Found server aliases {} for key type {}.", Arrays.toString(aliases), keyType);
            return aliases;
        }

        String[] fallbacks = fallbacksFor(keyType);
        for (String fallback : fallbacks) {
            aliases = delegate.getServerAliases(fallback, issuers);
            if (aliases != null) {
                logOutcome(outcomes, "serverAliases", keyType, Arrays.toString(aliases) + " via " + fallback,
                        "Found server aliases {} for key type {} using fallback key type {}.",
                        Arrays.toString(aliases), keyType, fallback);
                return aliases;
            }
        }

        logOutcome(outcomes, "serverAliases", keyType, null,
                "No server aliases for key type {}. Fallback key types tried: {}.",
                keyType, Arrays.toString(fallbacks));
        return null;
    }

    /**
     * Logs at info when the outcome of this lookup differs from the last one recorded for the same
     * kind and key type (including the first one), otherwise at debug.
     */
    private static void logOutcome(Map<String, String> outcomes, String kind, String keyType, String outcome,
                                   String message, Object... args) {
        String previous = outcomes.put(kind + ":" + keyType, String.valueOf(outcome));
        if (String.valueOf(outcome).equals(previous)) {
            log.debug(message, args);
        } else {
            log.info(message, args);
        }
    }

    private static String[] fallbacksFor(String keyType) {
        return SERVER_KEY_TYPE_FALLBACKS.getOrDefault(keyType, new String[0]);
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
                lastReloadSucceeded = true;
                lastLoggedOutcomes.clear();
                log.info("Successfully reloaded keystore.");
                return;
            }
        }

        throw new SSLException("No X509KeyManager in KeyManagerFactory.");
    }
}
