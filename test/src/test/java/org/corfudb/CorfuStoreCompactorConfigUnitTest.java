package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.compactor.CorfuStoreCompactorConfig;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.util.NodeLocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Slf4j
public class CorfuStoreCompactorConfigUnitTest {
    private static final String hostname = "hostname";
    private static final int port = 9000;
    private static final String keystore = "keystore";
    private static final String truststore = "truststore";
    private static final String ks_password = "ks_password";
    private static final String truststore_password = "truststore_password";
    private static final int bulkReadSize = 50;

    @Test
    public void testCompactorConfigSuccess() {
        final String CMD = "--hostname=" + hostname + " --port=" + port + " --trim=true" +
                " --upgradeDescriptorTable=true --startCheckpointing=true --instantTriggerCompaction=true " +
                "--freezeCompaction=true --disableCompaction=true --tlsEnabled=true --bulkReadSize=" + bulkReadSize +
                " --keystore=" + keystore + " --ks_password=" + ks_password + " --truststore=" + truststore +
                " --truststore_password=" + truststore_password;

        CorfuStoreCompactorConfig corfuStoreCompactorConfig = new CorfuStoreCompactorConfig(CMD.split(" "));

        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().tlsEnabled(true).keyStore(keystore)
                .ksPasswordFile(ks_password).trustStore(truststore).tsPasswordFile(truststore_password)
                .maxWriteSize(CorfuStoreCompactorConfig.DEFAULT_CP_MAX_WRITE_SIZE).bulkReadSize(bulkReadSize)
                .priorityLevel(PriorityLevel.HIGH)
                .systemDownHandlerTriggerLimit(CorfuStoreCompactorConfig.SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(corfuStoreCompactorConfig.getDefaultSystemDownHandler())
                .clientName(hostname)
                .cacheDisabled(true)
                .build();

        Assert.assertEquals(Optional.empty(), corfuStoreCompactorConfig.getPersistedCacheRoot());
        Assert.assertEquals(NodeLocator.builder().host(hostname).port(port).build(), corfuStoreCompactorConfig.getNodeLocator());
        Assert.assertTrue(corfuStoreCompactorConfig.isUpgradeDescriptorTable());
        Assert.assertTrue(corfuStoreCompactorConfig.isInstantTriggerCompaction());
        Assert.assertTrue(corfuStoreCompactorConfig.isTrim());
        Assert.assertTrue(corfuStoreCompactorConfig.isFreezeCompaction());
        Assert.assertTrue(corfuStoreCompactorConfig.isDisableCompaction());
        Assert.assertTrue(corfuStoreCompactorConfig.isStartCheckpointing());
        Assert.assertEquals(PriorityLevel.HIGH, corfuStoreCompactorConfig.getParams().getPriorityLevel());
        Assert.assertEquals(params, corfuStoreCompactorConfig.getParams());
    }

    @Test
    public void testCompactorConfigException() {
        final String CMD1 = "--hostname=" + hostname + " --port=" + port + " --trim=true" +
                " --upgradeDescriptorTable=true --startCheckpointing=true --instantTriggerCompaction=true " +
                "--freezeCompaction=true --unfreezeCompaction=true --tlsEnabled=true --bulkReadSize=" + bulkReadSize +
                " --keystore=" + keystore + " --ks_password=" + ks_password + " --truststore=" + truststore +
                " --truststore_password=" + truststore_password;
        Exception actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CorfuStoreCompactorConfig(CMD1.split(" "));
        });
        assertTrue(actualException.getMessage().contentEquals("Both freeze and unfreeze compaction parameters cannot be passed together"));

        final String CMD2 = "--hostname=" + hostname + " --port=" + port + " --trim=true" +
                " --upgradeDescriptorTable=true --startCheckpointing=true --instantTriggerCompaction=true " +
                "--enableCompaction=true --disableCompaction=true --tlsEnabled=true --bulkReadSize=" + bulkReadSize +
                " --keystore=" + keystore + " --ks_password=" + ks_password + " --truststore=" + truststore +
                " --truststore_password=" + truststore_password;
        actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CorfuStoreCompactorConfig(CMD2.split(" "));
        });
        assertTrue(actualException.getMessage().contentEquals("Both enable and disable compaction parameters cannot be passed together"));
    }
}
