package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Slf4j
public class CompactorConfigUnitTest {
    private static final String hostname = "hostname";
    private static final int port = 9000;
    private static final String keystore = "keystore";
    private static final String truststore = "truststore";
    private static final String ks_password = "ks_password";
    private static final String truststore_password = "truststore_password";
    private static final int bulkReadSize = 20;
    private static final int maxCacheEntries = 20;
    private static final String SPACE = " ";
    private final String baseCmd = "--hostname=" + hostname + " --port=" + port +
            " --tlsEnabled=true --bulkReadSize=" + bulkReadSize +
            " --maxCacheEntries=" + maxCacheEntries +
            " --keystore=" + keystore + " --ks_password=" +
            ks_password + " --truststore=" + truststore + " --truststore_password=" + truststore_password;

    @Test
    public void testCompactorControlsConfigSuccess() {
        final String cmd = baseCmd + " --freezeCompaction=true --disableCompaction=true --trim=true" +
                " --upgradeDescriptorTable=true --instantTriggerCompaction=true";

        CompactorControllerConfig corfuCompactorControlsConfig = new CompactorControllerConfig(cmd.split(SPACE));

        CorfuRuntimeParameters params = CorfuRuntimeParameters.builder().tlsEnabled(true).keyStore(keystore)
                .ksPasswordFile(ks_password).trustStore(truststore).tsPasswordFile(truststore_password)
                .maxWriteSize(CompactorBaseConfig.DEFAULT_CP_MAX_WRITE_SIZE).bulkReadSize(bulkReadSize)
                .priorityLevel(PriorityLevel.HIGH)
                .systemDownHandlerTriggerLimit(CompactorBaseConfig.SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT)
                .systemDownHandler(corfuCompactorControlsConfig.getDefaultSystemDownHandler())
                .clientName(hostname)
                .cacheDisabled(false)
                .maxCacheEntries(bulkReadSize)
                .maxMvoCacheEntries(0)
                .build();

        assertEquals(Optional.empty(), corfuCompactorControlsConfig.getPersistedCacheRoot());
        assertEquals(NodeLocator.builder().host(hostname).port(port).build(), corfuCompactorControlsConfig.getNodeLocator());
        assertTrue(corfuCompactorControlsConfig.isUpgradeDescriptorTable());
        assertTrue(corfuCompactorControlsConfig.isInstantTriggerCompaction());
        assertTrue(corfuCompactorControlsConfig.isTrim());
        assertTrue(corfuCompactorControlsConfig.isFreezeCompaction());
        assertTrue(corfuCompactorControlsConfig.isDisableCompaction());
        assertEquals(PriorityLevel.HIGH, corfuCompactorControlsConfig.getParams().getPriorityLevel());
        assertEquals(params, corfuCompactorControlsConfig.getParams());
    }

    @Test
    public void testCompactorConfigException() {
        final String cmd1 = baseCmd + " --freezeCompaction=true --unfreezeCompaction=true";
        Exception actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CompactorControllerConfig(cmd1.split(SPACE));
        });
        assertTrue(actualException.getMessage().contentEquals("Both freeze and unfreeze compaction parameters cannot be passed together"));

        final String cmd2 = baseCmd + " --enableCompaction=true --disableCompaction=true";
        actualException = assertThrows(IllegalArgumentException.class, () -> {
            new CompactorControllerConfig(cmd2.split(SPACE));
        });
        assertTrue(actualException.getMessage().contentEquals("Both enable and disable compaction parameters cannot be passed together"));
    }
}
