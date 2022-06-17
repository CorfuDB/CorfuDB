package org.corfudb.security.tls;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig;

import javax.net.ssl.KeyManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Utilities for common options parsing and session configuration for
 * encrypted & authenticated TLS sessions.
 */
@Slf4j
public class TlsUtils {
    public static final String PASSWORD_FILE_NOT_FOUND_ERROR = "Password file not found";

    private TlsUtils() {
        // prevent instantiation of this class
    }

    /**
     * Opens KeyStore or TrustStore.
     * Both trustStores and keyStores are represented by the KeyStore object
     *
     * @param certStoreCfg certificate store config
     * @return KeyStore keyStore object taken asynchronously
     */
    public static CompletableFuture<KeyStore> openCertStore(CertStoreConfig certStoreCfg) {
        return CompletableFuture
                //validation step
                .runAsync(certStoreCfg::checkCertStoreExists)
                .thenCompose(empty -> getCertStorePassword(certStoreCfg.getPasswordFile()))
                .thenApply(certStorePassword -> {
                    KeyStore keyStore = getKeyStoreInstance();

                    Path certStore = certStoreCfg.getCertStore();
                    FileInputStream inputStream = null;
                    try {
                        inputStream = new FileInputStream(certStore.toFile());
                        keyStore.load(inputStream, certStorePassword.toCharArray());
                        return keyStore;
                    } catch (IOException e) {
                        String errorMessage = String.format("Unable to read key store file %s.", certStore);
                        throw new IllegalStateException(errorMessage, e);
                    } catch (CertificateException e) {
                        String errorMessage = String.format("Unable to read certificates in key store file %s.", certStore);
                        throw new IllegalStateException(errorMessage, e);
                    } catch (NoSuchAlgorithmException e) {
                        String errorMessage = String.format("No support for algorithm [%s] in key store %s.",
                                KeyStore.getDefaultType(), certStore);
                        throw new IllegalStateException(errorMessage, e);
                    } finally {
                        Consumer<IOException> emptyExceptionHandler = ex -> {
                        };
                        IOUtils.closeQuietly(inputStream, emptyExceptionHandler);
                    }
                });
    }

    private static KeyStore getKeyStoreInstance() {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        } catch (KeyStoreException e) {
            String errorMessage = String.format("Error creating a key store object of algorithm [%s].",
                    KeyStore.getDefaultType());
            throw new IllegalStateException(errorMessage, e);
        }

        return keyStore;
    }

    public static CompletableFuture<String> getCertStorePassword(Path passwordFilePath) {
        return CompletableFuture.supplyAsync(() -> {
            if (!Files.exists(passwordFilePath)) {
                throw new IllegalStateException(PASSWORD_FILE_NOT_FOUND_ERROR);
            }

            String password;
            try {
                password = new String(Files.readAllBytes(passwordFilePath)).trim();
            } catch (IOException e) {
                String errorMessage = "Unable to read password file " + passwordFilePath + ".";
                throw new IllegalStateException(errorMessage, e);
            }

            if (password.isEmpty()) {
                throw new IllegalStateException("Empty password");
            }

            return password;
        });
    }

    public static CompletableFuture<KeyManagerFactory> createKeyManagerFactory(KeyStoreConfig cfg) {

        CompletableFuture<String> passwordAsync = getCertStorePassword(cfg.getPasswordFile());
        CompletableFuture<KeyStore> keyStoreAsync = TlsUtils.openCertStore(cfg);

        return passwordAsync.thenCombine(keyStoreAsync, (password, keyStore) -> {
                    KeyManagerFactory kmf;
                    try {
                        kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(keyStore, password.toCharArray());
                        return kmf;
                    } catch (UnrecoverableKeyException e) {
                        String errorMessage = "Unrecoverable key in key store " + cfg.getKeyStoreFile() + ".";
                        throw new IllegalStateException(errorMessage, e);
                    } catch (NoSuchAlgorithmException e) {
                        String errorMessage = "Can not create key manager factory with default algorithm "
                                + KeyManagerFactory.getDefaultAlgorithm() + ".";
                        throw new IllegalStateException(errorMessage, e);
                    } catch (KeyStoreException e) {
                        String errorMessage = "Can not initialize key manager factory from " + cfg.getKeyStoreFile() + ".";
                        throw new IllegalStateException(errorMessage, e);
                    }
                });
    }

    /**
     * Java key store configuration class
     * https://www.baeldung.com/java-keystore-truststore-difference
     */
    public interface CertStoreConfig {

        Path getCertStore();

        default void checkCertStoreExists() {
            if (Files.notExists(getCertStore())) {
                String errorMessage = String.format("Key store file {%s} doesn't exist.", getCertStore());
                throw new IllegalStateException(errorMessage);
            }
        }

        Path getPasswordFile();

        @AllArgsConstructor
        @Getter
        @ToString
        class KeyStoreConfig implements CertStoreConfig {
            private final Path keyStoreFile;
            private final Path passwordFile;

            public static KeyStoreConfig from(String keyStorePath, String passwordFile) {
                return new KeyStoreConfig(Paths.get(keyStorePath), Paths.get(passwordFile));
            }

            @Override
            public Path getCertStore() {
                return keyStoreFile;
            }
        }

        @AllArgsConstructor
        @Getter
        @ToString
        class TrustStoreConfig implements CertStoreConfig {
            /**
             * TrustStore path
             */
            private final Path trustStoreFile;
            private final Path passwordFile;

            public static TrustStoreConfig from(String trustStorePath, String passwordFile) {
                return new TrustStoreConfig(Paths.get(trustStorePath), Paths.get(passwordFile));
            }

            @Override
            public Path getCertStore() {
                return trustStoreFile;
            }
        }
    }
}
