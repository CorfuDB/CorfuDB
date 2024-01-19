package org.corfudb.security.tls;

import io.netty.handler.ssl.util.SimpleKeyManagerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.ManagerFactoryParameters;
import java.security.KeyStore;

public class ReloadableKeyManagerFactory extends SimpleKeyManagerFactory {

    private final ReloadableKeyManager reloadableKeyManager;

    public ReloadableKeyManagerFactory(TlsUtils.CertStoreConfig.KeyStoreConfig keyStoreConfig) {
        reloadableKeyManager = new ReloadableKeyManager(keyStoreConfig);
    }

    @Override
    protected void engineInit(KeyStore keyStore, char[] var2) throws Exception {
        //inherited, don't do anything
    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws Exception {
        //inherited, don't do anything
    }

    @Override
    protected KeyManager[] engineGetKeyManagers() {
        return new KeyManager[] {reloadableKeyManager};
    }
}
