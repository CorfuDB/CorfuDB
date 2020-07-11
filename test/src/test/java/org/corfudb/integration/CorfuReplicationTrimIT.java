package org.corfudb.integration;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This suite of tests verifies the behavior of log replication in the event of
 * Checkpoint and Trim both in the sender (source/active cluster) and receiver (sink/standby cluster)
 */
public class CorfuReplicationTrimIT extends LogReplicationAbstractIT {

    /**
     * Sets the plugin path before starting any test
     * @throws Exception
     */
    @Before
    public void setupPluginPath() {
        if(runProcess) {
            File f = new File(nettyConfig);
            this.pluginConfigFilePath = f.getAbsolutePath();
        } else {
            this.pluginConfigFilePath = nettyConfig;
        }
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
            checkpointAndTrimStandby();

            // Write Entry's to Active Cluster (while replicator is down)
            System.out.println("Write additional entries to active CorfuDB ...");
            writeToActive((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.size()).isEqualTo(numWrites*2);

            // Confirm new data does not exist on Standby Cluster
            assertThat(mapAStandby.size()).isEqualTo((numWrites + (numWrites/2)));

            // Checkpoint & Trim on the Active so we force a snapshot sync on restart
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
     * Test the case where the log is trimmed in between log entry sync.
     * In this test we stop the active log replicator before trimming so we
     * can enforce re-negotiation after the trim.
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySync() throws Exception {
        testLogTrimBetweenLogEntrySync(true);
    }

    /**
     * Test the case where the log is trimmed in between log entry sync.
     * In this test we don't stop the active log replicator, so the
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySyncContinuous() throws Exception {
        testLogTrimBetweenLogEntrySync(false);
    }

    /**
     * Test trimming the log on the standby site, in the middle of log entry sync.
     *
     * @param stop true, stop the active server right before trimming (to enforce re-negotiation)
     *             false, trim without stopping the server.
     */
    private void testLogTrimBetweenLogEntrySync(boolean stop) throws Exception {
        try {
            testEndToEndSnapshotAndLogEntrySync();

            if (stop) {
                // Stop Log Replication on Active, so we can test re-negotiation in the event of trims
                System.out.println("Stop Active Log Replicator ...");
                stopActiveLogReplicator();
            }

            // Checkpoint & Trim on the Standby, so we trim the shadow stream
            checkpointAndTrimStandby();

            // Write Entry's to Active Cluster (while replicator is down)
            System.out.println("Write additional entries to active CorfuDB ...");
            writeToActive((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.size()).isEqualTo(numWrites*2);

            if (stop) {
                // Confirm new data does not exist on Standby Cluster
                assertThat(mapAStandby.size()).isEqualTo((numWrites + (numWrites / 2)));

                System.out.println("Start active Log Replicator again ...");
                startActiveLogReplicator();
            }

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
