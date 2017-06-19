package org.corfudb.security.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Map;
import java.util.function.Consumer;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * Utilities for common options parsing and session configuration for
 * encrypted & authenticated TLS sessions.
 */

public class TlsUtils {
    public enum SslContextType { SERVER_CONTEXT, CLIENT_CONTEXT }

    /**
     * Create SslContext object based on Getopt-style parameter spec.
     *
     * @param desiredType Server or client context
     * @param opts Getopt-style parameters
     * @param keyStoreException Consumer for key store error
     * @param ksPasswordFileException Consumer for ks password file error
     * @param trustStoreException Consumer for trust store error
     * @param tsPasswordFileException Consumer for ts password file error
     * @return SslContext object or null on error
     */
    public static SslContext enableTls(SslContextType desiredType,
                                       Map<String, Object> opts,
                                       Consumer<Exception> keyStoreException,
                                       Consumer<Exception> ksPasswordFileException,
                                       Consumer<Exception> trustStoreException,
                                       Consumer<Exception> tsPasswordFileException) {
        return enableTls(desiredType,
                (String) opts.get("--keystore"), keyStoreException,
                (String) opts.get("--keystore-password-file"), ksPasswordFileException,
                (String) opts.get("--truststore"), trustStoreException,
                (String) opts.get("--truststore-password-file"), tsPasswordFileException);
    }

    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param desiredType Server or client context
     * @param keyStore Key store path string
     * @param keyStoreException Consumer for key store error
     * @param ksPasswordFile Key store password file string
     * @param ksPasswordFileException Consumer for ks password file error
     * @param trustStore Trust store path string
     * @param trustStoreException Consumer for trust store error
     * @param tsPasswordFile Trust store password file path string
     * @param tsPasswordFileException Consumer for ts password file error
     * @return SslContext object or null on error
     */
    public static SslContext enableTls(SslContextType desiredType,
                                       String keyStore,
                                       Consumer<Exception> keyStoreException,
                                       String ksPasswordFile,
                                       Consumer<Exception> ksPasswordFileException,
                                       String trustStore,
                                       Consumer<Exception> trustStoreException,
                                       String tsPasswordFile,
                                       Consumer<Exception> tsPasswordFileException) {
        // Get the key store password
        String ksp = "";
        if (ksPasswordFile != null) {
            try {
                ksp = (new String(Files.readAllBytes(Paths.get(ksPasswordFile))))
                        .trim();
            } catch (Exception e) {
                keyStoreException.accept(e);
                return null;
            }
        }
        // Get the key store
        KeyStore ks = null;
        if (keyStore != null) {
            try (FileInputStream fis = new FileInputStream(keyStore)) {
                ks = KeyStore.getInstance(KeyStore.getDefaultType());
                ks.load(fis, ksp.toCharArray());
            } catch (Exception e) {
                ksPasswordFileException.accept(e);
                return null;
            }
        }

        // Get the trust store password
        String tsp = "";
        if (tsPasswordFile != null) {
            try {
                tsp = (new String(Files.readAllBytes(Paths.get(tsPasswordFile))))
                        .trim();
            } catch (Exception e) {
                trustStoreException.accept(e);
                return null;
            }
        }
        // Get the trust store
        KeyStore ts = null;
        if (trustStore != null) {
            try (FileInputStream fis = new FileInputStream(trustStore)) {
                ts = KeyStore.getInstance(KeyStore.getDefaultType());
                ts.load(fis, tsp.toCharArray());
            } catch (Exception e) {
                tsPasswordFileException.accept(e);
                return null;
            }
        }

        try {
            KeyManagerFactory kmf =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, ksp.toCharArray());
            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            switch (desiredType) {
                case CLIENT_CONTEXT:
                    return SslContextBuilder.forClient().keyManager(kmf).trustManager(tmf).build();
                case SERVER_CONTEXT:
                    return SslContextBuilder.forServer(kmf).trustManager(tmf).build();
                default:
                    throw new RuntimeException("Bad SSL context type: " + desiredType);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not build SslContext type "
                    + desiredType.toString() + ": "
                    + e.getClass().getSimpleName(), e);
        }
    }

}
