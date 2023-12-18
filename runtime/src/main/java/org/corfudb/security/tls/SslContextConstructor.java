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

@Slf4j
public class SslContextConstructor {
    /**
     * Create SslContext object based on a spec of individual configuration strings.
     *
     * @param isServer Server or client
     * @param keyStoreConfig Key store path
     * @param trustStoreConfig Trust store path
     * @return SslContext object.
     * @throws SSLException Wrapper exception for any issue reading the key/trust store.
     */
    public static SslContext constructSslContext(
            boolean isServer, KeyStoreConfig keyStoreConfig, TrustStoreConfig trustStoreConfig) throws SSLException {
        log.info("Construct ssl context based on the following information:");
        log.info("Key store file path: {}.", keyStoreConfig.getKeyStoreFile());
        log.info("Key store password file path: {}.", keyStoreConfig.getPasswordFile());
        log.info("Trust store file path: {}.", trustStoreConfig.getTrustStoreFile());
        log.info("Trust store password file path: {}.", trustStoreConfig.getPasswordFile());

        KeyManagerFactory kmf = new ReloadableKeyManagerFactory(keyStoreConfig);
        ReloadableTrustManagerFactory tmf = new ReloadableTrustManagerFactory(trustStoreConfig);

        SslProvider provider = getSslProvider();

        if (isServer) {
            return SslContextBuilder
                    .forServer(kmf)
                    .sslProvider(provider)
                    .trustManager(tmf)
                    .build();
        } else {
            return SslContextBuilder
                    .forClient()
                    .sslProvider(provider)
                    .keyManager(kmf)
                    .trustManager(tmf)
                    .build();
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
