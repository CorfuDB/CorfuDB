package org.corfudb.security.tls;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;

/**
 * A trust manager factory that returns a ReloadableTrustManager.
 *
 * Created by zjohnny on 9/19/17.
 */
public class ReloadableTrustManagerFactory extends SimpleTrustManagerFactory {

    private ReloadableTrustManager trustManager;

    /**
     * Constructor.
     *
     * @param trustStorePath
     *          Location of trust store.
     * @param trustPasswordPath
     *          Location of trust store password.
     * @throws SSLException
     *          Thrown when there's an issue with loading the trust store.
     */
    public ReloadableTrustManagerFactory(String trustStorePath, String trustPasswordPath) throws SSLException {
        trustManager = new ReloadableTrustManager(trustStorePath, trustPasswordPath);
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