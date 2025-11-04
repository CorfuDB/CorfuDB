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
        SslContext sslContext;
        if (isServer) {
            sslContext = SslContextBuilder
                    .forServer(kmf)
                    .sslProvider(provider)
                    .trustManager(tmf)
                    .build();
        } else {
            sslContext = SslContextBuilder
                    .forClient()
                    .sslProvider(provider)
                    .keyManager(kmf)
                    .trustManager(tmf)
                    .build();
        }
        log.debug("Created ssl context {}. isServer {}, type is {}",
                sslContext, isServer, sslContext.getClass().getName());
        return sslContext;
    }

    private static SslProvider getSslProvider() {
        SslProvider provider = SslProvider.JDK;

        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
            log.info("Using OpenSSL provider (native): version={}", OpenSsl.versionString());
        } else {
            log.warn("OpenSSL is NOT available, falling back to JDK SSL. Reason: {}",
                    OpenSsl.unavailabilityCause() != null ? OpenSsl.unavailabilityCause().getMessage() : "unknown");
            if (OpenSsl.unavailabilityCause() != null) {
                log.debug("OpenSSL unavailable - full stack trace:", OpenSsl.unavailabilityCause());
            }
        }
        return provider;
    }
}
