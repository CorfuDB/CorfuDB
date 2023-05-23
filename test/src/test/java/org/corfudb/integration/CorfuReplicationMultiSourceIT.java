package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
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
 * This IT uses a topology of 3 Source clusters and 1 Sink cluster, each with its own Corfu Server.  The 3 Source
 * clusters replicate to the same Sink cluster.  The set of streams to replicate as received from the
 * LogReplicationConfig is the same for all clusters.
 */
@Slf4j
@RunWith(Parameterized.class)
public class CorfuReplicationMultiSourceIT extends CorfuReplicationMultiSourceSinkIT {

    public CorfuReplicationMultiSourceIT(String pluginConfigFilePath) {
        this.pluginConfigFilePath = pluginConfigFilePath;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and
    // GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList(
            "src/test/resources/transport/pluginConfig.properties"
        );

        List<String> absolutePathPlugins = new ArrayList<>();
        transportPlugins.forEach(plugin -> {
            File f = new File(plugin);
            absolutePathPlugins.add(f.getAbsolutePath());
        });
        return absolutePathPlugins;
    }

    @Before
    public void setUp() throws Exception {
        // Setup Corfu on 3 LR Source Sites and 1 LR Sink Site
        super.setUp(MAX_REMOTE_CLUSTERS, 1, DefaultClusterManager.TP_MULTI_SOURCE);
    }

    /**
     * The test verifies snapshot and log entry sync on a topology with 3 Source clusters and 1 Sink cluster.
     * The 3 Source clusters replicate to the same Sink cluster.  The set of streams to replicate as received from the
     * LogReplicationConfig is the same for all clusters.
     * However, Source Cluster 1 has data in Table001, Source Cluster 2 in Table002 and Source Cluster 3 in Table003.
     *
     * During snapshot sync, a 'clear' on Table002 from Source Cluster 1 can overwrite the updates written by Source
     * Cluster 2 because the streams to replicate set is the same.  In the test, a listener listens to updates on
     * each table and filters out 'single clear' entries during snapshot sync and collects the other updates.  It then
     * verifies that the required number of updates were received on the expected table.
     *
     * Verification of entries being replicated during log entry sync is straightforward and needs no such filtering.
     *
     * The test also verifies that the expected number of writes were made to the ReplicationStatus table on the Sink.
     * The number of updates depends on the number of available Source clusters(3 in this case).
     *
     */
    @Test
    public void testUpdatesOnReplicatedTables() throws Exception {
        // Setup Corfu on 3 LR Source Sites and 1 LR Sink Site
        super.setUp(MAX_REMOTE_CLUSTERS, 1, DefaultClusterManager.TP_MULTI_SOURCE);
        verifySnapshotAndLogEntrySink(false);
    }

    /**
     * Same as testUpdatesOnReplicatedTables(), but the sink is the one which starts the connection
     */
    @Test
    public void testUpdatesOnReplicatedTables_sinkConnectionStarter() throws Exception {
        // Setup Corfu on 3 LR Source Sites and 1 LR Sink Site
        super.setUp(MAX_REMOTE_CLUSTERS, 1, DefaultClusterManager.TP_MULTI_SOURCE_REV_CONNECTION);
        verifySnapshotAndLogEntrySink(false);
    }

    /**
     * Verify snapshot and log entry sync in a topology with 3 Source clusters and 1 Sink cluster.
     * Then perform a role switch by changing the topology to 3 Sink clusters, 1 Source cluster and verify that the new
     * Sink clusters successfully complete a snapshot sync.
     * @throws Exception
     */
    @Test
    public void testRoleChange() throws Exception {
        super.setUp(MAX_REMOTE_CLUSTERS, 1, DefaultClusterManager.TP_MULTI_SOURCE);
        verifySnapshotAndLogEntrySink(false);
        log.info("Preparing for role change");
        prepareTestTopologyForRoleChange(1, MAX_REMOTE_CLUSTERS);
        log.info("Testing after role change");
        verifySnapshotAndLogEntrySink(true);
    }
}
