package org.corfudb.integration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class CorfuReplicationE2EIT extends LogReplicationAbstractIT {

    public CorfuReplicationE2EIT(String plugin) {
        this.pluginConfigFilePath = plugin;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList(
                "src/test/resources/transport/grpcConfig.properties",
                "src/test/resources/transport/nettyConfig.properties");

        if(runProcess) {
            List<String> absolutePathPlugins = new ArrayList<>();
            transportPlugins.forEach(plugin -> {
                File f = new File(plugin);
                absolutePathPlugins.add(f.getAbsolutePath());
            });

            return absolutePathPlugins;
        }

        return transportPlugins;
    }

    /**
     * Test Log Replication End to End for snapshot and log entry sync. These tests emulate two sites,
     * one active and one standby. The active site is represented by one Corfu Database and one LogReplication Server,
     * and the standby the same. Data is written into the active datastore and log replication is initiated to test
     * snapshot sync and afterwards incremental updates are written to evaluate log entry sync.
     *
     * The transport (communication) layer is based on a plugin architecture. We have two sample plugins:
     * - GRPC
     * - Netty
     *
     * This is a parameterized test and both plugins are tested.
     *
     * @throws Exception
     */
    @Test
    public void testLogReplicationEndToEnd() throws Exception {
        testEndToEndSnapshotAndLogEntrySync();
    }
}
