package org.corfudb.security.tls;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.SslProvider;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SslContextConstructor {
    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param isServer Server or client
     * @param keyStorePath Key store path string
     * @param ksPasswordFile Key store password file string
     * @param trustStorePath Trust store path string
     * @param tsPasswordFile Trust store password file path string
     * @return SslContext object.
     * @throws SSLException
     *          Wrapper exception for any issue reading the key/trust store.
     */
    public static SslContext constructSslContext(boolean isServer,
                                                 @NonNull String keyStorePath,
                                                 String ksPasswordFile,
                                                 @NonNull String trustStorePath,
                                                 String tsPasswordFile) throws SSLException {
        log.trace("Construct ssl context based on the following information:");
        log.trace("Key store file path: {}.", keyStorePath);
        log.trace("Key store password file path: {}.", ksPasswordFile);
        log.trace("Trust store file path: {}.", trustStorePath);
        log.trace("Trust store password file path: {}.", tsPasswordFile);

        KeyManagerFactory kmf = createKeyManagerFactory(keyStorePath, ksPasswordFile);
        ReloadableTrustManagerFactory tmf = new ReloadableTrustManagerFactory(trustStorePath, tsPasswordFile);

        SslProvider provider = SslProvider.JDK;

        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
        } else {
            log.warn("constructSslContext: couldn't load native openssl library, using JdkSslEngine instead!");
        }


        if (isServer) {
            return SslContextBuilder.forServer(kmf).sslProvider(provider).trustManager(tmf).build();
        } else {
            return SslContextBuilder.forClient().sslProvider(provider).keyManager(kmf).trustManager(tmf).build();
        }
    }

    private static KeyManagerFactory createKeyManagerFactory(String keyStorePath,
                                                             String ksPasswordFile) throws SSLException {
        String keyStorePassword = TlsUtils.getKeyStorePassword(ksPasswordFile);
        KeyStore keyStore = TlsUtils.openKeyStore(keyStorePath, keyStorePassword);

        KeyManagerFactory kmf;
        try {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyStorePassword.toCharArray());
            return kmf;
        } catch (UnrecoverableKeyException e) {
            String errorMessage = "Unrecoverable key in key store " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (NoSuchAlgorithmException e) {
            String errorMessage = "Can not create key manager factory with default algorithm "
                    + KeyManagerFactory.getDefaultAlgorithm() + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        } catch (KeyStoreException e) {
            String errorMessage = "Can not initialize key manager factory from " + keyStorePath + ".";
            log.error(errorMessage, e);
            throw new SSLException(errorMessage, e);
        }
    }
}
