package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This suite of tests verifies the behavior of log replication in the event of
 * Checkpoint and Trim both in the sender (source/active cluster) and receiver (sink/standby cluster)
 */
@Slf4j
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
     * Test the case where the log is trimmed on the standby in between two snapshot syncs.
     * We should guarantee that shadow streams do not throw trim exceptions and data is applied successfully.
     *
     * This test does the following:
     *      (1) Do a Snapshot (full) and Log Entry (delta) Sync
     *      (2) Stop the active cluster LR (we stop so we can write some more data and checkpoint it, such that we enforce
     *          s subsequent snapshot sync, otherwise this written data would be transferred in log entry--delta--sync)
     *      (3) Checkpoint and Trim on Standby Cluster (enforce shadow streams to be trimmed)
     *      (4) Write additional data on Active Cluster (while LR is down)
     *          (4.1) Verify Data written is present on active
     *          (4.2) Verify this new Data is not yet present on standby
     *      (5) Checkpoint and Trim on Active Cluster (to guarantee when we bring up the server, delta's are not available
     *          so Snapshot Sync is enforced)
     *      (6) Start Active LR
     *      (7) Verify Data reaches standby cluster
     *
     *
     */
    @Test
    public void testTrimBetweenSnapshotSync() throws Exception {
        try {
            testEndToEndSnapshotAndLogEntrySync();

            // Stop Log Replication on Active, so we can write some data into active Corfu
            // and checkpoint so we enforce a subsequent Snapshot Sync
            log.debug("Stop Active Log Replicator ...");
            stopActiveLogReplicator();

            // Checkpoint & Trim on the Standby (so shadow stream get trimmed)
            checkpointAndTrim(false);

            // Write Entry's to Active Cluster (while replicator is down)
            log.debug("Write additional entries to active CorfuDB ...");
            writeToActiveNonUFO((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.count()).isEqualTo(numWrites*2);

            // Confirm new data does not exist on Standby Cluster
            assertThat(mapAStandby.count()).isEqualTo(numWrites + (numWrites / 2));

            // Checkpoint & Trim on the Active so we force a snapshot sync on restart
            checkpointAndTrim(true);

            log.debug("Start active Log Replicator again ...");
            startActiveLogReplicator();

            log.debug("Verify Data on Standby ...");
            verifyDataOnStandbyNonUFO((numWrites*2));
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
     * Test the case where the log is trimmed in between two cycles of log entry sync.
     * In this test we stop the active log replicator before trimming so we
     * can enforce re-negotiation after the trim.
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySync() throws Exception {
        testLogTrimBetweenLogEntrySync(true);
    }

    /**
     * Test the case where the log is trimmed in between two cycles of log entry sync.
     * In this test we don't stop the active log replicator, so log entry sync is resumed.
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
                log.debug("Stop Active Log Replicator ...");
                stopActiveLogReplicator();
            }

            // Checkpoint & Trim on the Standby, so we trim the shadow stream
            checkpointAndTrim(false);

            // Write Entry's to Active Cluster (while replicator is down)
            log.debug("Write additional entries to active CorfuDB ...");
            writeToActiveNonUFO((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.count()).isEqualTo(numWrites*2);

            if (stop) {
                // Confirm new data does not exist on Standby Cluster
                assertThat(mapAStandby.count()).isEqualTo(numWrites + (numWrites / 2));

                log.debug("Start active Log Replicator again ...");
                startActiveLogReplicator();
            }

            // Since we did not checkpoint data should be transferred in delta's
            log.debug("Verify Data on Standby ...");
            verifyDataOnStandbyNonUFO((numWrites*2));
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

    @Test
    public void testSnapshotSyncEndToEndWithCheckpointedStreams() throws Exception {
        try {
            log.debug("\nSetup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            log.debug("Open map on active and standby");
            openMap();

            log.debug("Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActiveNonUFO(0, numWrites);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.count()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Cluster
            assertThat(mapAStandby.count()).isZero();

            // Checkpoint and Trim Before Starting
            checkpointAndTrim(true);

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandbyNonUFO(numWrites);
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
