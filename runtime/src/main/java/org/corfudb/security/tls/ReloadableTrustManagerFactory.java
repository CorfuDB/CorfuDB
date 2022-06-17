package org.corfudb.security.tls;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;
import org.corfudb.security.tls.TlsUtils.CertStoreConfig.TrustStoreConfig;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;

/**
 * A trust manager factory that returns a ReloadableTrustManager.
 *
 */
public class ReloadableTrustManagerFactory extends SimpleTrustManagerFactory {

    private final ReloadableTrustManager trustManager;

    /**
     * Constructor.
     *
     * @param trustStoreConfig Location of trust store.
     */
    public ReloadableTrustManagerFactory(TrustStoreConfig trustStoreConfig) {
        trustManager = new ReloadableTrustManager(trustStoreConfig);
    }

    @Override
    protected void engineInit(KeyStore keyStore) throws KeyStoreException {
        //inherited, don't do anything
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws InvalidAlgorithmParameterException {
        //inherited, don't do anything
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[] { trustManager };
    }
}