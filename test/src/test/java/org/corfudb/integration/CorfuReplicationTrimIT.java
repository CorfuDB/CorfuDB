package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This suite of tests verifies the behavior of log replication in the event of
 * Checkpoint and Trim both in the sender (source cluster) and receiver (sink cluster)
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
            File f = new File(pluginConfigFilePath);
            this.pluginConfigFilePath = f.getAbsolutePath();
        }
    }

    /**
     * Test the case where the log is trimmed on the sink in between two snapshot syncs.
     * We should guarantee that shadow streams do not throw trim exceptions and data is applied successfully.
     *
     * This test does the following:
     *      (1) Do a Snapshot (full) and Log Entry (delta) Sync
     *      (2) Stop the source cluster LR (we stop so we can write some more data and checkpoint it, such that we enforce
     *          s subsequent snapshot sync, otherwise this written data would be transferred in log entry--delta--sync)
     *      (3) Checkpoint and Trim on Sink Cluster (enforce shadow streams to be trimmed)
     *      (4) Write additional data on Source Cluster (while LR is down)
     *          (4.1) Verify Data written is present on source
     *          (4.2) Verify this new Data is not yet present on sink
     *      (5) Checkpoint and Trim on Source Cluster (to guarantee when we bring up the server, delta's are not available
     *          so Snapshot Sync is enforced)
     *      (6) Start Source LR
     *      (7) Verify Data reaches sink cluster
     *
     *
     */
    @Test
    public void testTrimBetweenSnapshotSync() throws Exception {
        try {
            testEndToEndSnapshotAndLogEntrySync();

            // Stop Log Replication on Source, so we can write some data into source Corfu
            // and checkpoint so we enforce a subsequent Snapshot Sync
            log.debug("Stop Source Log Replicator ...");
            stopSourceLogReplicator();

            // Checkpoint & Trim on the Sink (so shadow stream get trimmed)
            checkpointAndTrim(false);

            // Write Entry's to Source Cluster (while replicator is down)
            log.debug("Write additional entries to source CorfuDB ...");
            writeToSourceNonUFO((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Source Cluster
            assertThat(mapA.count()).isEqualTo(numWrites*2);

            // Confirm new data does not exist on Sink Cluster
            assertThat(mapASink.count()).isEqualTo(numWrites + (numWrites / 2));

            // Checkpoint & Trim on the Source so we force a snapshot sync on restart
            checkpointAndTrim(true);

            log.debug("Start source Log Replicator again ...");
            startSourceLogReplicator();

            log.debug("Verify Data on Sink ...");
            verifyDataOnSinkNonUFO((numWrites*2));
        } finally {

            executorService.shutdownNow();

            if (sourceCorfu != null) {
                sourceCorfu.destroy();
            }

            if (sinkCorfu != null) {
                sinkCorfu.destroy();
            }

            if (sourceReplicationServer != null) {
                sourceReplicationServer.destroy();
            }

            if (sinkReplicationServer != null) {
                sinkReplicationServer.destroy();
            }
        }
    }

    /**
     * Test the case where the log is trimmed in between two cycles of log entry sync.
     * In this test we stop the source log replicator before trimming so we
     * can enforce re-negotiation after the trim.
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySync() throws Exception {
        testLogTrimBetweenLogEntrySync(true);
    }

    /**
     * Test the case where the log is trimmed in between two cycles of log entry sync.
     * In this test we don't stop the source log replicator, so log entry sync is resumed.
     */
    @Test
    public void testTrimmedExceptionsBetweenLogEntrySyncContinuous() throws Exception {
        testLogTrimBetweenLogEntrySync(false);
    }

    /**
     * Test trimming the log on the sink site, in the middle of log entry sync.
     *
     * @param stop true, stop the source server right before trimming (to enforce re-negotiation)
     *             false, trim without stopping the server.
     */
    private void testLogTrimBetweenLogEntrySync(boolean stop) throws Exception {
        try {
            testEndToEndSnapshotAndLogEntrySync();

            if (stop) {
                // Stop Log Replication on Source, so we can test re-negotiation in the event of trims
                log.debug("Stop Source Log Replicator ...");
                stopSourceLogReplicator();
            }

            // Checkpoint & Trim on the Sink, so we trim the shadow stream
            checkpointAndTrim(false);

            // Write Entry's to Source Cluster (while replicator is down)
            log.debug("Write additional entries to source CorfuDB ...");
            writeToSourceNonUFO((numWrites + (numWrites/2)), numWrites/2);

            // Confirm data does exist on Source Cluster
            assertThat(mapA.count()).isEqualTo(numWrites*2);

            if (stop) {
                // Confirm new data does not exist on Sink Cluster
                assertThat(mapASink.count()).isEqualTo(numWrites + (numWrites / 2));

                log.debug("Start source Log Replicator again ...");
                startSourceLogReplicator();
            }

            // Since we did not checkpoint data should be transferred in delta's
            log.debug("Verify Data on Sink ...");
            verifyDataOnSinkNonUFO((numWrites*2));
        } finally {

            executorService.shutdownNow();

            if (sourceCorfu != null) {
                sourceCorfu.destroy();
            }

            if (sinkCorfu != null) {
                sinkCorfu.destroy();
            }

            if (sourceReplicationServer != null) {
                sourceReplicationServer.destroy();
            }

            if (sinkReplicationServer != null) {
                sinkReplicationServer.destroy();
            }
        }
    }

    @Test
    public void testSnapshotSyncEndToEndWithCheckpointedStreams() throws Exception {
        try {
            log.debug("\nSetup source and sink Corfu's");
            setupSourceAndSinkCorfu();

            log.debug("Open map on source and sink");
            openMap();

            log.debug("Write data to source CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToSourceNonUFO(0, numWrites);

            // Confirm data does exist on Source Cluster
            assertThat(mapA.count()).isEqualTo(numWrites);

            // Confirm data does not exist on Sink Cluster
            assertThat(mapASink.count()).isZero();

            // Checkpoint and Trim Before Starting
            checkpointAndTrim(true);

            //initiate the topology buckets with a single source/sink topology.
            // This needs to be done after checkpoint trim as defaultClusterManager starts the listener at trimMark
            initSingleSourceSinkCluster();

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnSinkNonUFO(numWrites);
        } finally {
            executorService.shutdownNow();

            if (sourceCorfu != null) {
                sourceCorfu.destroy();
            }

            if (sinkCorfu != null) {
                sinkCorfu.destroy();
            }

            if (sourceReplicationServer != null) {
                sourceReplicationServer.destroy();
            }

            if (sinkReplicationServer != null) {
                sinkReplicationServer.destroy();
            }
        }
    }
}
