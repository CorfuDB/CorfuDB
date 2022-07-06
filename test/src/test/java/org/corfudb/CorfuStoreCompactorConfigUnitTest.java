package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.compactor.CorfuStoreCompactorConfig;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.util.NodeLocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

@Slf4j
public class CorfuStoreCompactorConfigUnitTest {
    private static final String hostname = "hostname";
    private final static int port = 9000;
    private final static String keystore = "keystore";
    private final static String truststore = "truststore";
    private final static String ks_password = "ks_password";
    private final static String truststore_password = "truststore_password";
    private final static int bulkReadSize = 50;
    private final static String CMD = "--hostname=" + hostname + " --port=" + port + " --trim=true" +
            " --isUpgrade --tlsEnabled=true --bulkReadSize=" + bulkReadSize + " --keystore=" + keystore +
            " --ks_password=" + ks_password +" --truststore=" + truststore + " --truststore_password=" + truststore_password;

    @Test
    public void testCorfuStoreCompactorConfig() {
        CorfuStoreCompactorConfig corfuStoreCompactorConfig = new CorfuStoreCompactorConfig(CMD.split(" "));

        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().tlsEnabled(true).keyStore(keystore)
                .ksPasswordFile(ks_password).trustStore(truststore).tsPasswordFile(truststore_password)
                .maxWriteSize(CorfuStoreCompactorConfig.DEFAULT_CP_MAX_WRITE_SIZE).bulkReadSize(bulkReadSize)
                .systemDownHandlerTriggerLimit(CorfuStoreCompactorConfig.SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(corfuStoreCompactorConfig.getDefaultSystemDownHandler()).build();

        Assert.assertEquals(Optional.empty(), corfuStoreCompactorConfig.getPersistedCacheRoot());
        Assert.assertEquals(NodeLocator.builder().host(hostname).port(port).build(), corfuStoreCompactorConfig.getNodeLocator());
        Assert.assertTrue(corfuStoreCompactorConfig.isUpgrade());
        Assert.assertEquals(params, corfuStoreCompactorConfig.getParams());
    }
}
