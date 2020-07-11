package org.corfudb.integration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

    /**
     * Test the case where the log is trimmed in between two snapshot syncs.
     * We should guarantee that shadow streams do not throw trim exceptions
     * and data is applied successfully.
     */
    @Test
    public void testTrimmedExceptionsBetweenSnapshotSync() throws Exception {
        try {
            // TODO: these tests need the lock lease duration to be decreased, so when
            //   restarting the log replication it acquires the lease right back, the problem is
            //   our runnerServer accepts only strings and it expects an int.
            testEndToEndSnapshotAndLogEntrySync();

            // Stop Log Replication on Active, so we can write some data into active Corfu
            // and checkpoint so we enforce a subsequent Snapshot Sync
            System.out.println("Stop Active Log Replicator ...");
            stopActiveLogReplicator();

            // Checkpoint & Trim on the Standby (so shadow stream gets
            checkpointStandby();

            // Write Entry's to Active Cluster (while replicator is down)
            System.out.println("Write additional entries to active CorfuDB ...");
            writeToActive((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.size()).isEqualTo(numWrites*2);

            // Confirm new data does not exist on Standby Cluster
            assertThat(mapAStandby.size()).isEqualTo((numWrites + (numWrites/2)));

            // Checkpoint & Trim on the Active so we force a snapshot sync on restart
            // checkpointActive();
            checkpointAndTrimCorfuStore();

            System.out.println("Start active Log Replicator again ...");
            startActiveLogReplicator();

            System.out.println("Verify Data on Standby ...");
            verifyDataOnStandby((numWrites*2));

            System.out.println("Entries :: " + mapAStandby.keySet());

        } finally {

            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }

    /**
     * Test the case where the log is trimmed in between two log entry syncs.
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySync() throws Exception {
        try {

            testLogReplicationEndToEnd();

            // Stop Log Replication on Active, so we can write some data into active Corfu
            // and checkpoint so we enforce a subsequent Snapshot Sync
            System.out.println("Stop Active Log Replicator ...");
            stopActiveLogReplicator();

            // Checkpoint & Trim on the Standby, so we trim the shadow stream
            checkpointStandby();

            // Write Entry's to Active Cluster (while replicator is down)
            System.out.println("Write additional entries to active CorfuDB ...");
            writeToActive((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.size()).isEqualTo(numWrites*2);

            // Confirm new data does not exist on Standby Cluster
            assertThat(mapAStandby.size()).isEqualTo((numWrites + (numWrites/2)));

            System.out.println("Start active Log Replicator again ...");
            startActiveLogReplicator();

            // Since we did not checkpoint data should be transferred in delta's
            System.out.println("Verify Data on Standby ...");
            verifyDataOnStandby((numWrites*2));

            System.out.println("Entries :: " + mapAStandby.keySet());
        } finally {

            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }
}
