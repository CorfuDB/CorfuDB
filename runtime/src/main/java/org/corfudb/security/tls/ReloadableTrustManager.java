package org.corfudb.security.tls;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * This trust manager reloads the trust store whenever a checkClientTrusted
 * or checkServerTrusted is called.
 */
@Slf4j
public class ReloadableTrustManager implements X509TrustManager {
    private final TrustStoreWatcher watcher;

    /**
     * Constructor.
     *
     * @param trustStoreConfig Location of trust store.
     */
    public ReloadableTrustManager(TrustStoreConfig trustStoreConfig) {
        watcher = new TrustStoreWatcher(trustStoreConfig);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        X509TrustManager trustManager = getTrustManager();
        trustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        X509TrustManager trustManager = getTrustManager();
        trustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    private X509TrustManager getTrustManager() throws CertificateException {
        X509TrustManager trustManager;
        try {
            trustManager = watcher.getTrustManager();
        } catch (Exception e) {
            throw new CertificateException(e);
        }
        return trustManager;
    }

    public static class TrustStoreWatcher {
        @NonNull
        private final TrustStoreConfig trustStoreConfig;

        private final ExecutorService executor = Executors.newSingleThreadExecutor();

        private CompletableFuture<TrustManagerContext> trustManagerAsync;

        public TrustStoreWatcher(@NonNull TrustStoreConfig trustStoreConfig) {
            this.trustStoreConfig = trustStoreConfig;
            trustManagerAsync = loadTrustStore();
            //init trust manager synchronously
            getTrustManager();
        }

        public CompletableFuture<TrustManagerContext> reloadTrustManagerAsync() {
            trustManagerAsync = reloadTrustStore();
            return trustManagerAsync;
        }

        private X509TrustManager getTrustManager() {
            X509TrustManager tm;
            try {
                tm = reloadTrustManagerAsync().join().trustManager;
            } catch (CompletionException e) {
                throw new IllegalStateException(e.getCause());
            }
            return tm;
        }

        /**
         * Reload the trust manager.
         */
        private CompletableFuture<TrustManagerContext> reloadTrustStore() {
            //Reload trust store in case of it get expired
            return trustManagerAsync.thenCompose(ctx -> {
                if (ctx.isNotExpired()) {
                    return CompletableFuture.completedFuture(ctx);
                }

                return loadTrustStore();
            });
        }

        private CompletableFuture<TrustManagerContext> loadTrustStore() {
            return TlsUtils
                    .openCertStore(trustStoreConfig)
                    .thenComposeAsync(this::loadTrustManager);
        }

        private CompletableFuture<TrustManagerContext> loadTrustManager(KeyStore trustStore) {
            Supplier<TrustManagerContext> asyncLoader = () -> {
                TrustManagerFactory tmf;
                try {
                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                } catch (NoSuchAlgorithmException e) {
                    String errorMessage = "No support for TrustManagerFactory default algorithm "
                            + TrustManagerFactory.getDefaultAlgorithm() + ".";
                    throw new CompletionException(new CertificateException(errorMessage, e));
                } catch (KeyStoreException e) {
                    String errorMessage = "Unable to load trust store " + trustStoreConfig.getTrustStoreFile() + ".";
                    throw new CompletionException(new CertificateException(errorMessage, e));
                }

                for (TrustManager tm : tmf.getTrustManagers()) {
                    if (tm instanceof X509TrustManager) {
                        return new TrustManagerContext((X509TrustManager) tm, LocalDateTime.now());
                    }
                }

                throw new CompletionException(new CertificateException("No X509TrustManager in TrustManagerFactory."));
            };

            return CompletableFuture.supplyAsync(asyncLoader, executor);
        }

        @AllArgsConstructor
        static class TrustManagerContext {
            private static final Duration TIMEOUT = Duration.ofSeconds(3);

            @NonNull
            private final X509TrustManager trustManager;
            private final LocalDateTime finishTime;

            public boolean isNotExpired() {
                return !isExpired();
            }

            public boolean isExpired() {
                LocalDateTime now = LocalDateTime.now();
                return finishTime.plus(TIMEOUT).isBefore(now);
            }
        }
    }
}
