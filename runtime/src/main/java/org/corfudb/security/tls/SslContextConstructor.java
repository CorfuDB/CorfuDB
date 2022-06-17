package org.corfudb.security.tls;

import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.KeyStoreConfig;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
public class SslContextConstructor {
    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param isServer         Server or client
     * @param keyStoreConfig   Key store path
     * @param trustStoreConfig Trust store path
     * @return SslContext object.
     */
    public static SslContext constructSslContext(
            boolean isServer, KeyStoreConfig keyStoreConfig, TrustStoreConfig trustStoreConfig) {
        log.info("Construct ssl context based on the following information:");
        log.info("Key store file path: {}.", keyStoreConfig.getKeyStoreFile());
        log.info("Key store password file path: {}.", keyStoreConfig.getPasswordFile());
        log.info("Trust store file path: {}.", trustStoreConfig.getTrustStoreFile());
        log.info("Trust store password file path: {}.", trustStoreConfig.getPasswordFile());

        CompletableFuture<KeyManagerFactory> kmfAsync = TlsUtils.createKeyManagerFactory(keyStoreConfig);
        ReloadableTrustManagerFactory tmf = new ReloadableTrustManagerFactory(trustStoreConfig);

        SslProvider provider = getSslProvider();

        KeyManagerFactory kmf;
        try {
            kmf = kmfAsync.join();
        } catch (CompletionException e) {
            throw (IllegalStateException) e.getCause();
        }

        SslContextBuilder sslContextBuilder;
        if (isServer) {
            sslContextBuilder = SslContextBuilder
                    .forServer(kmf)
                    .sslProvider(provider)
                    .trustManager(tmf);
        } else {
            sslContextBuilder = SslContextBuilder
                    .forClient()
                    .sslProvider(provider)
                    .keyManager(kmf)
                    .trustManager(tmf);
        }

        try {
            return sslContextBuilder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Can't build SSL context", e);
        }
    }

    private static SslProvider getSslProvider() {
        SslProvider provider = SslProvider.JDK;

        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
        } else {
            log.warn("constructSslContext: couldn't load native openssl library, using JdkSslEngine instead!");
        }
        return provider;
    }
}
