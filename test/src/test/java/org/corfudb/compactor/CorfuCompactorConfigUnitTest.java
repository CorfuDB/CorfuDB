package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.util.NodeLocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Slf4j
public class CorfuCompactorConfigUnitTest {
    private static final String hostname = "hostname";
    private static final int port = 9000;
    private static final String keystore = "keystore";
    private static final String truststore = "truststore";
    private static final String ks_password = "ks_password";
    private static final String truststore_password = "truststore_password";
    private static final int bulkReadSize = 50;
    private final String baseCmd = "--hostname=" + hostname + " --port=" + port +
            " --tlsEnabled=true --bulkReadSize=" + bulkReadSize + " --keystore=" + keystore + " --ks_password=" +
            ks_password + " --truststore=" + truststore + " --truststore_password=" + truststore_password;

    @Test
    public void testCompactorControlsConfigSuccess() {
        final String cmd = baseCmd + " --freezeCompaction=true --disableCompaction=true --trim=true" +
                " --upgradeDescriptorTable=true --instantTriggerCompaction=true" ;

        CorfuCompactorControlsConfig corfuCompactorControlsConfig = new CorfuCompactorControlsConfig(cmd.split(" "));

        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().tlsEnabled(true).keyStore(keystore)
                .ksPasswordFile(ks_password).trustStore(truststore).tsPasswordFile(truststore_password)
                .maxWriteSize(CorfuCompactorConfig.DEFAULT_CP_MAX_WRITE_SIZE).bulkReadSize(bulkReadSize)
                .priorityLevel(PriorityLevel.HIGH)
                .systemDownHandlerTriggerLimit(CorfuCompactorConfig.SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(corfuCompactorControlsConfig.getDefaultSystemDownHandler())
                .clientName(hostname)
                .cacheDisabled(true)
                .build();

        Assert.assertEquals(Optional.empty(), corfuCompactorControlsConfig.getPersistedCacheRoot());
        Assert.assertEquals(NodeLocator.builder().host(hostname).port(port).build(), corfuCompactorControlsConfig.getNodeLocator());
        Assert.assertTrue(corfuCompactorControlsConfig.isUpgradeDescriptorTable());
        Assert.assertTrue(corfuCompactorControlsConfig.isInstantTriggerCompaction());
        Assert.assertTrue(corfuCompactorControlsConfig.isTrim());
        Assert.assertTrue(corfuCompactorControlsConfig.isFreezeCompaction());
        Assert.assertTrue(corfuCompactorControlsConfig.isDisableCompaction());
        Assert.assertEquals(PriorityLevel.HIGH, corfuCompactorControlsConfig.getParams().getPriorityLevel());
        Assert.assertEquals(params, corfuCompactorControlsConfig.getParams());
    }

    @Test
    public void testCompactorConfigException() {
        final String cmd1 = baseCmd + " --freezeCompaction=true --unfreezeCompaction=true";
        Exception actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CorfuCompactorControlsConfig(cmd1.split(" "));
        });
        assertTrue(actualException.getMessage().contentEquals("Both freeze and unfreeze compaction parameters cannot be passed together"));

        final String cmd2 = baseCmd + " --enableCompaction=true --disableCompaction=true";
        actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CorfuCompactorControlsConfig(cmd2.split(" "));
        });
        assertTrue(actualException.getMessage().contentEquals("Both enable and disable compaction parameters cannot be passed together"));
    }
}
