package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * This IT uses a topology of 3 Sink clusters and 1 Source cluster, each with its own Corfu Server.  The Source
 * cluster replicates to all 3 Sink clusters.  The set of streams to replicate as received from the
 * LogReplicationConfig is the same for all clusters.
 */
@Slf4j
@RunWith(Parameterized.class)
public class CorfuReplicationMultiSinkIT extends CorfuReplicationMultiSourceSinkIT {

    public CorfuReplicationMultiSinkIT(String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and
    // GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList(
            "src/test/resources/transport/grpcConfig.properties",
            "src/test/resources/transport/nettyConfig.properties");

        List<String> absolutePathPlugins = new ArrayList<>();
        transportPlugins.forEach(plugin -> {
            File f = new File(plugin);
            absolutePathPlugins.add(f.getAbsolutePath());
        });

        return absolutePathPlugins;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp(1, MAX_REMOTE_CLUSTERS);
    }

    /**
     * The test verifies snapshot and log entry sync on a topology with 1 Source clusters and 3 Sink clusters.
     * The Source cluster replicates to all Sink clusters.  The set of streams to replicate as received from the
     * LogReplicationConfig is the same for all clusters.
     * Source Cluster 1 only has data in Table001.  This data will get replicated to all Sink clusters.
     *
     * The test verifies that the required number of updates were received on Table001 on all Sink clusters.
     * The test also verifies that the expected number of writes were made to the ReplicationStatus table on the Sink.
     * The number of updates depends on the number of available Source clusters(3 in this case).
     *
     * Later, 1 update and 1 delete(separate transactions) are performed on Table001 on the Source.  The test
     * verifies that they were applied correctly.
     */
    @Test
    public void testUpdatesOnReplicatedTables() throws Exception {
        verifySnapshotAndLogEntrySink(false);
    }

    @Test
    public void testRoleChange() throws Exception {
        verifySnapshotAndLogEntrySink(false);
        log.info("Preparing for role change");
        prepareTestTopologyForRoleChange(MAX_REMOTE_CLUSTERS, 1);
        log.info("Testing after role change");
        verifySnapshotAndLogEntrySink(true);
    }
}
