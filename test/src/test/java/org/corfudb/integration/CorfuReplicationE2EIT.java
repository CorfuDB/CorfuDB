package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.runtime.ExampleSchemas;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Slf4j
@RunWith(Parameterized.class)
public class CorfuReplicationE2EIT extends LogReplicationAbstractIT {

    public CorfuReplicationE2EIT(Pair<String, ExampleSchemas.ClusterUuidMsg> pluginAndTopologyType) {
        this.pluginConfigFilePath = pluginAndTopologyType.getKey();
        this.topologyType = pluginAndTopologyType.getValue();
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and GRPC)
    @Parameterized.Parameters
    public static Collection<Pair<String, ExampleSchemas.ClusterUuidMsg>> input() {

        List<String> transportPlugins = Arrays.asList(
                "src/test/resources/transport/grpcConfig.properties"
                //"src/test/resources/transport/nettyConfig.properties"
        );

        List<ExampleSchemas.ClusterUuidMsg> topologyTypes = Arrays.asList(
                DefaultClusterManager.TP_SINGLE_SOURCE_SINK,
                DefaultClusterManager.TP_SINGLE_SOURCE_SINK_REV_CONNECTION
        );

        List<Pair<String, ExampleSchemas.ClusterUuidMsg>> absolutePathPlugins = new ArrayList<>();

        if(runProcess) {
            transportPlugins.stream().map(File::new).forEach(f ->
                    topologyTypes.stream().forEach(type -> absolutePathPlugins.add(Pair.of(f.getAbsolutePath(), type))));
        } else {

            transportPlugins.stream().forEach(f ->
                    topologyTypes.stream().forEach(type -> absolutePathPlugins.add(Pair.of(f, type))));
        }

        return absolutePathPlugins;
    }

    /**
     * Test Log Replication End to End for snapshot and log entry sync. These tests emulate two sites,
     * one source and one sink. The source site is represented by one Corfu Database and one LogReplication Server,
     * and the sink the same. Data is written into the source datastore and log replication is initiated to test
     * snapshot sync and afterwards incremental updates are written to evaluate log entry sync.
     *
     * The transport (communication) layer is based on a plugin architecture. We have two sample plugins:
     * - GRPC
     * - Netty
     *
     * This is a parameterized test and both plugins are tested.
     *
     * @throws Exception error
     */
    @Test
    public void testLogReplicationEndToEnd() throws Exception {
        log.debug("Using plugin :: {}", pluginConfigFilePath);
        testEndToEndSnapshotAndLogEntrySyncUFO(false, true, 1, true);
    }

    @Test
    public void testSnapshotSyncMultipleTables() throws Exception {
        log.debug("Using transport :: {}", transportType);
        final int totalNumMaps = 3;
        testEndToEndSnapshotAndLogEntrySyncUFO(totalNumMaps, false, true, 1, true);
    }

    @Test
    public void testDiskBasedLogReplicationEndToEnd() throws Exception {
        log.debug("Using plugin :: {}", pluginConfigFilePath);
        testEndToEndSnapshotAndLogEntrySyncUFO(true, true, 1, true);
    }

    @Test
    public void testDiskBasedSnapshotSyncMultipleTables() throws Exception {
        log.debug("Using plugin :: {}", transportType);
        final int totalNumMaps = 3;
        testEndToEndSnapshotAndLogEntrySyncUFO(totalNumMaps, true, true, 1, true);
    }
}
