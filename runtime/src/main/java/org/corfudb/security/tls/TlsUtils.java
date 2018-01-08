package org.corfudb.security.tls;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/**
 * Utilities for common options parsing and session configuration for
 * encrypted & authenticated TLS sessions.
 */
@Slf4j
public class TlsUtils {
    /**
     * Open up a key store. Wraps all the exceptions.
     *
     * @param keyStorePath Key store path string
     * @param password Key store password
     * @return SslContext object or null on error
     * @throws SSLException
     *          Thrown when there's an issue with loading the trust store.
     */
    public static KeyStore openKeyStore(String keyStorePath,
                                        String password) throws SSLException {
        File keyStoreFile = new File(keyStorePath);
        if (!keyStoreFile.exists()) {
            String errorMessage = "Key store file {" + keyStorePath + "} doesn't exist.";
            log.error(errorMessage);
            throw new SSLException(errorMessage);
        }

        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(keyStorePath);
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(inputStream, password.toCharArray());
            return keyStore;
        } catch (IOException e) {
            String errorMessage = "Unable to read key store file " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (CertificateException e) {
            String errorMessage = "Unable to read certificates in key store file " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "No support for algorithm [" + KeyStore.getDefaultType() +
                    "] in key store " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Error creating a key store object of algorithm ["
                    + KeyStore.getDefaultType() + "].";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    /**
     * Get the password from file. Wrap any exception with SSLException.
     *
     * @param passwordFilePath
     *      Path to password file.
     * @return If file path is provided, returns the content, otherwise
     *      an empty string.
     * @throws SSLException
     *      Wraps the IOException.
     */
    public static String getKeyStorePassword(String passwordFilePath) throws SSLException {
        if (passwordFilePath == null || passwordFilePath.isEmpty()) {
            return "";
        }
        try {
            return (new String(Files.readAllBytes(Paths.get(passwordFilePath)))).trim();
        } catch (IOException e) {
            String errorMessage = "Unable to read password file " + passwordFilePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        }
    }
}
