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
    private static final Map<String, String[]> SERVER_KEY_TYPE_FALLBACKS = Map.of(
            "EC", new String[]{"ECDHE_ECDSA"},
            "EC_EC", new String[]{"ECDH_ECDSA"},
            "RSA", new String[]{"ECDHE_RSA", "DHE_RSA"},
            "EC_RSA", new String[]{"ECDH_RSA"},
            "DSA", new String[]{"DHE_DSA"}
    );

    private final TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig;

    private X509KeyManager keyManager;

    private long keysLastModifiedTime;

    private boolean lastReloadSucceeded;

    /**
     * Last logged outcome of each alias lookup, shared by all instances. Corfu builds a new SSL
     * context, and so a new key manager, for every channel, so state kept per instance would never
     * suppress anything. Entries are keyed by key store file, lookup kind and key type, so the
     * number of entries is bounded by the key stores in use and the handful of key types asked for.
     */
    private static final Map<String, String> SHARED_OUTCOMES = new ConcurrentHashMap<>();

    private final OutcomeLog outcomeLog;

    /**
     * Logs the outcome of an alias lookup at info when it is the first one, differs from the last
     * one logged for the same key store file, lookup kind and key type, or follows a key store
     * reload. Identical repeats are logged at debug, because alias lookups run on every handshake.
     */
    static final class OutcomeLog {
        private final Map<String, String> lastOutcomes;
        private final String keyStore;
        private volatile long keyStoreVersion;

        OutcomeLog(Map<String, String> lastOutcomes, String keyStore) {
            this.lastOutcomes = lastOutcomes;
            this.keyStore = keyStore;
        }

        void setKeyStoreVersion(long keyStoreVersion) {
            this.keyStoreVersion = keyStoreVersion;
        }

        void log(String kind, String keyType, String outcome, String message, Object... args) {
            // The key store version is part of the value, so a new certificate is logged again
            // without any entry having to be removed.
            long version = keyStoreVersion;
            String current = outcome + "@" + version;
            String previous = lastOutcomes.put(keyStore + "|" + kind + "|" + keyType, current);

            // The version is the key store modification time, the same value that is printed when the
            // key store is reloaded or skipped, so a line can be matched with the key store it used.
            Object[] allArgs = Arrays.copyOf(args, args.length + 1);
            allArgs[args.length] = version;
            String fullMessage = message + " Key store version: {}.";
            if (current.equals(previous)) {
                log.debug(fullMessage, allArgs);
            } else {
                log.info(fullMessage, allArgs);
            }
        }
    }

    public ReloadableKeyManager(TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig) {
        this.keyStoreConfig = keyStoreConfig;
        this.outcomeLog = new OutcomeLog(SHARED_OUTCOMES, String.valueOf(keyStoreConfig.getKeyStoreFile()));
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
        return chooseClientAlias(outcomeLog, keyManager, keyType, issuers, socket);
    }

    /**
     * BCJSSE accepts the same key type names Netty uses on the client side (RSA, DSA, EC), so no
     * fallback is needed here. Only log the outcome to trace which alias was picked.
     */
    static String chooseClientAlias(OutcomeLog outcomes, X509KeyManager delegate, String[] keyType,
                                    Principal[] issuers, Socket socket) {
        String alias = delegate.chooseClientAlias(keyType, issuers, socket);
        outcomes.log("client", Arrays.toString(keyType), String.valueOf(alias),
                "Client alias for key types {}: {}.", Arrays.toString(keyType), alias);
        return alias;
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        reloadKeyStoreWrapper();
        return getServerAliases(outcomeLog, keyManager, keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        reloadKeyStoreWrapper();
        return chooseServerAlias(outcomeLog, keyManager, keyType, issuers, socket);
    }

    /**
     * Ask the delegate for the requested key type first so that providers which accept it keep
     * their behavior, and only then try the spellings BCJSSE uses for the same key family.
     * Names without a mapping (e.g. TLS 1.3 signature scheme names) are never rewritten.
     */
    static String chooseServerAlias(OutcomeLog outcomes, X509KeyManager delegate, String keyType, Principal[] issuers, Socket socket) {
        String alias = delegate.chooseServerAlias(keyType, issuers, socket);
        if (alias != null) {
            outcomes.log("chooseServer", keyType, alias,
                    "Chose server alias {} for key type {}.", alias, keyType);
            return alias;
        }

        String[] fallbacks = fallbacksFor(keyType);
        for (String fallback : fallbacks) {
            alias = delegate.chooseServerAlias(fallback, issuers, socket);
            if (alias != null) {
                outcomes.log("chooseServer", keyType, alias + " via " + fallback,
                        "Chose server alias {} for key type {} using fallback key type {}.",
                        alias, keyType, fallback);
                return alias;
            }
        }

        outcomes.log("chooseServer", keyType, null,
                "No server alias for key type {}. Fallback key types tried: {}.",
                keyType, Arrays.toString(fallbacks));
        return null;
    }

    /**
     * Same lookup order as {@link #chooseServerAlias(OutcomeLog, X509KeyManager, String, Principal[], Socket)},
     * so that listing aliases and choosing one agree on the key type names.
     */
    static String[] getServerAliases(OutcomeLog outcomes, X509KeyManager delegate, String keyType, Principal[] issuers) {
        String[] aliases = delegate.getServerAliases(keyType, issuers);
        if (aliases != null) {
            outcomes.log("serverAliases", keyType, Arrays.toString(aliases),
                    "Found server aliases {} for key type {}.", Arrays.toString(aliases), keyType);
            return aliases;
        }

        String[] fallbacks = fallbacksFor(keyType);
        for (String fallback : fallbacks) {
            aliases = delegate.getServerAliases(fallback, issuers);
            if (aliases != null) {
                outcomes.log("serverAliases", keyType, Arrays.toString(aliases) + " via " + fallback,
                        "Found server aliases {} for key type {} using fallback key type {}.",
                        Arrays.toString(aliases), keyType, fallback);
                return aliases;
            }
        }

        outcomes.log("serverAliases", keyType, null,
                "No server aliases for key type {}. Fallback key types tried: {}.",
                keyType, Arrays.toString(fallbacks));
        return null;
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
                outcomeLog.setKeyStoreVersion(keysLastModifiedTime);
                log.info("Successfully reloaded keystore.");
                return;
            }
        }

        throw new SSLException("No X509KeyManager in KeyManagerFactory.");
    }
}
