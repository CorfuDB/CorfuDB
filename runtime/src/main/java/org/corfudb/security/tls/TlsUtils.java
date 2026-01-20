package org.corfudb.security.tls;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.function.Consumer;

/**
 * Utilities for common options parsing and session configuration for
 * encrypted & authenticated TLS sessions.
 */
@Slf4j
public class TlsUtils {
    public static final String PASSWORD_FILE_NOT_FOUND_ERROR = "Password file not found";
    private static final Logger log = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    // Keystore type constants
    public static final String KEYSTORE_TYPE_BCFKS = "BCFKS";
    public static final String KEYSTORE_TYPE_JKS = "JKS";
    public static final String KEYSTORE_TYPE_PKCS12 = "PKCS12";

    // Provider constants
    public static final String PROVIDER_BCFIPS = "BCFIPS";
    public static final String PROVIDER_SUN = "SUN";

    private TlsUtils() {
        // prevent instantiation of this class
    }

    /**
     * Check if BCFIPS provider is available.
     *
     * @return true if BCFIPS provider is available, false otherwise
     */
    public static boolean isBcFipsProviderAvailable() {
        Provider bcfipsProvider = Security.getProvider(PROVIDER_BCFIPS);
        return bcfipsProvider != null;
    }

    /**
     * Check if SUN provider is available.
     *
     * @return true if SUN provider is available, false otherwise
     */
    public static boolean isSunProviderAvailable() {
        Provider sunProvider = Security.getProvider(PROVIDER_SUN);
        return sunProvider != null;
    }

    /**
     * Result of opening a certificate store, containing both the KeyStore and its type.
     */
    @AllArgsConstructor
    @Getter
    public static class KeyStoreResult {
        private final KeyStore keyStore;
        private final String keyStoreType;
        private final String provider;

        public boolean isBcFips() {
            return KEYSTORE_TYPE_BCFKS.equals(keyStoreType) && PROVIDER_BCFIPS.equals(provider);
        }
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
        return openCertStoreWithType(certStoreCfg).getKeyStore();
    }

    /**
     * Opens KeyStore or TrustStore and returns both the KeyStore and its type information.
     * This method handles the following scenarios based on available providers:
     * <ul>
     *   <li>When both BCFIPS and SUN providers are present: can load both BCFKS and JKS keystores</li>
     *   <li>When only SUN provider is present (no BCFIPS): can only load JKS keystores, not BCFKS</li>
     *   <li>When only BCFIPS provider is present (no SUN): can only load BCFKS keystores, not JKS</li>
     * </ul>
     *
     * @param certStoreCfg certificate store config
     * @return KeyStoreResult containing the KeyStore and type information
     * @throws SSLException Thrown when there's an issue with loading the cert store.
     */
    public static KeyStoreResult openCertStoreWithType(CertStoreConfig certStoreCfg) throws SSLException {
        certStoreCfg.checkCertStoreExists();
        Path certStore = certStoreCfg.getCertStore();

        log.info("TlsUtils.openCertStore: Attempting to load keystore from: {}", certStore);
        
        String certStorePassword = getKeyStorePassword(certStoreCfg.getPasswordFile());

        // Check which providers are available
        boolean isBcFipsProviderAvailable = isBcFipsProviderAvailable();
        boolean isSunProviderAvailable = isSunProviderAvailable();
        
        log.info("TlsUtils: Provider availability - BCFIPS: {}, SUN: {}", isBcFipsProviderAvailable, isSunProviderAvailable);
        logAvailableProviders();

        if (!isBcFipsProviderAvailable && !isSunProviderAvailable) {
            throw new SSLException("No supported security providers available (neither BCFIPS nor SUN)");
        }

        // Track errors for better error reporting
        Exception bcfksError = null;
        Exception jksError = null;

        FileInputStream inputStream = null;
        try {
            // Try BCFKS format if BCFIPS provider is available
            if (isBcFipsProviderAvailable) {
                log.info("TlsUtils: Attempting to load as BCFKS with BCFIPS provider");
                try {
                    inputStream = new FileInputStream(certStore.toFile());
                    
                    KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE_BCFKS, PROVIDER_BCFIPS);
                    log.info("TlsUtils: KeyStore.getInstance(\"{}\", \"{}\") succeeded", 
                            KEYSTORE_TYPE_BCFKS, PROVIDER_BCFIPS);
                    
                    keyStore.load(inputStream, certStorePassword.toCharArray());
                    log.info("TlsUtils: Successfully loaded BCFKS keystore from: {}", certStore);
                    return new KeyStoreResult(keyStore, KEYSTORE_TYPE_BCFKS, PROVIDER_BCFIPS);
                } catch (NoSuchProviderException e) {
                    bcfksError = e;
                    log.info("TlsUtils: BCFIPS provider not available: {}", e.getMessage());
                } catch (KeyStoreException e) {
                    bcfksError = e;
                    log.info("TlsUtils: KeyStore error loading BCFKS: {}", e.getMessage());
                } catch (IOException e) {
                    bcfksError = e;
                    log.info("TlsUtils: IOException loading BCFKS (may not be BCFKS format): {} - {}", 
                            e.getClass().getName(), e.getMessage());
                } catch (Exception e) {
                    bcfksError = e;
                    log.info("TlsUtils: Unexpected error loading BCFKS: {} - {}", 
                        e.getClass().getName(), e.getMessage());
                } finally {
                    // Close the input stream before retry
                    Consumer<IOException> emptyExceptionHandler = ex -> {};
                    IOUtils.closeQuietly(inputStream, emptyExceptionHandler);
                    inputStream = null;
                }
            }
            
            // Try JKS/default format if SUN provider is available
            if (isSunProviderAvailable) {
                String defaultType = KeyStore.getDefaultType();
                log.info("TlsUtils: Attempting to load as {} (SUN provider available)", defaultType);
                
                try {
                    inputStream = new FileInputStream(certStore.toFile());
                    
                    KeyStore keyStore = KeyStore.getInstance(defaultType);
                    keyStore.load(inputStream, certStorePassword.toCharArray());
                    
                    // Determine the provider
                    String providerName = keyStore.getProvider() != null ? 
                            keyStore.getProvider().getName() : PROVIDER_SUN;
                    
                    log.info("TlsUtils: Successfully loaded {} keystore from: {} (provider: {})", 
                            defaultType, certStore, providerName);
                    return new KeyStoreResult(keyStore, defaultType, providerName);
                } catch (Exception e) {
                    jksError = e;
                    log.info("TlsUtils: Error loading {} keystore: {} - {}", 
                            defaultType, e.getClass().getName(), e.getMessage());
                } finally {
                    Consumer<IOException> emptyExceptionHandler = ex -> {};
                    IOUtils.closeQuietly(inputStream, emptyExceptionHandler);
                    inputStream = null;
                }
            }
            
            // If we reach here, all attempts failed - throw appropriate error
            String errorMessage = buildErrorMessage(certStore, isBcFipsProviderAvailable, isSunProviderAvailable, bcfksError, jksError);
            throw new SSLException(errorMessage);
            
        } finally {
            Consumer<IOException> emptyExceptionHandler = ex -> {};
            IOUtils.closeQuietly(inputStream, emptyExceptionHandler);
        }
    }

    /**
     * Builds an appropriate error message based on available providers and errors encountered.
     */
    private static String buildErrorMessage(Path certStore, boolean bcfipsAvailable, boolean sunAvailable,
                                            Exception bcfksError, Exception jksError) {
        StringBuilder sb = new StringBuilder();
        sb.append("Unable to load keystore from ").append(certStore).append(". ");
        
        if (bcfipsAvailable && sunAvailable) {
            // Both providers available but neither format worked
            sb.append("Tried both BCFKS and JKS formats. ");
            if (bcfksError != null) {
                sb.append("BCFKS error: ").append(bcfksError.getMessage()).append(". ");
            }
            if (jksError != null) {
                sb.append("JKS error: ").append(jksError.getMessage()).append(". ");
            }
        } else if (bcfipsAvailable && !sunAvailable) {
            // Only BCFIPS available - keystore must be BCFKS format
            sb.append("Only BCFIPS provider is available, keystore must be in BCFKS format. ");
            if (bcfksError != null) {
                sb.append("Error: ").append(bcfksError.getMessage());
            }
        } else if (!bcfipsAvailable && sunAvailable) {
            // Only SUN available - keystore must be JKS/PKCS12 format
            sb.append("Only SUN provider is available, keystore must be in JKS or PKCS12 format. ");
            if (jksError != null) {
                sb.append("Error: ").append(jksError.getMessage());
            }
        }
        
        return sb.toString();
    }

    private static void logAvailableProviders() {
        log.debug("TlsUtils: Available security providers:");
        for (Provider p : Security.getProviders()) {
            log.debug("TlsUtils:   - {} (version {})", p.getName(), p.getVersion());
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

