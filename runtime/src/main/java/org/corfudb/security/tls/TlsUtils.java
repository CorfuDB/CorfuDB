package org.corfudb.security.tls;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import javax.net.ssl.SSLException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
     * @return KeyStore object
     * @throws SSLException Thrown when there's an issue with loading the cert store.
     */
    public static KeyStore openCertStore(CertStoreConfig certStoreCfg) throws SSLException {
        certStoreCfg.checkCertStoreExists();
        Path certStore = certStoreCfg.getCertStore();

        String certStorePassword = getKeyStorePassword(certStoreCfg.getPasswordFile());

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(certStore.toFile());
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(inputStream, certStorePassword.toCharArray());
            return keyStore;
        } catch (IOException e) {
            String errorMessage = String.format("Unable to read key store file %s.", certStore);
            throw new SSLException(errorMessage, e);
        } catch (CertificateException e) {
            String errorMessage = String.format("Unable to read certificates in key store file %s.", certStore);
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = String.format("No support for algorithm [%s] in key store %s.",
                    KeyStore.getDefaultType(), certStore);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = String.format("Error creating a key store object of algorithm [%s].",
                    KeyStore.getDefaultType());
            throw new SSLException(errorMessage, e);
        } finally {
            Consumer<IOException> emptyExceptionHandler = ex -> {
            };
            IOUtils.closeQuietly(inputStream, emptyExceptionHandler);
        }
    }

    public static String getKeyStorePassword(Path passwordFilePath) throws SSLException {
        if (!Files.exists(passwordFilePath)) {
            throw new SSLException(PASSWORD_FILE_NOT_FOUND_ERROR);
        }

        String password;
        try {
            password = new String(Files.readAllBytes(passwordFilePath)).trim();
        } catch (IOException e) {
            String errorMessage = "Unable to read password file " + passwordFilePath + ".";
            throw new SSLException(errorMessage, e);
        }

        if (password.isEmpty()) {
            throw new SSLException("Empty password");
        }

        return password;
    }

    /**
     * Java key store configuration class
     * https://www.baeldung.com/java-keystore-truststore-difference
     */
    public interface CertStoreConfig {

        Path getCertStore();

        default void checkCertStoreExists() throws SSLException {
            if (Files.notExists(getCertStore())) {
                String errorMessage = String.format("Key store file {%s} doesn't exist.", getCertStore());
                throw new SSLException(errorMessage);
            }
        }

        Path getPasswordFile();

        @AllArgsConstructor
        @Getter
        @ToString
        class KeyStoreConfig implements CertStoreConfig {
            private final Path keyStoreFile;
            private final Path passwordFile;

            public static final boolean DEFAULT_DISABLE_FILE_WATCHER = false;

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
            public static final Path DEFAULT_DISABLE_CERT_EXPIRY_CHECK_FILE = Paths.get(
                    "/", "usr", "share", "corfu", "conf", "DISABLE_CERT_EXPIRY_CHECK"
            );

            /**
             * TrustStore path
             */
            private final Path trustStoreFile;
            private final Path passwordFile;
            private final Path disableCertExpiryCheckFile;

            public static TrustStoreConfig from(String trustStorePath, String passwordFile, Path disableCertExpiryCheckFile) {
                return new TrustStoreConfig(
                        Paths.get(trustStorePath),
                        Paths.get(passwordFile),
                        disableCertExpiryCheckFile
                );
            }

            @Override
            public Path getCertStore() {
                return trustStoreFile;
            }

            public boolean isCertExpiryCheckEnabled() {
                return !Files.exists(disableCertExpiryCheckFile);
            }
        }

        @AllArgsConstructor
        @Getter
        @ToString
        class CertManagementConfig {
            private final KeyStoreConfig keyStoreConfig;
            private final TrustStoreConfig trustStoreConfig;
        }
    }
}
