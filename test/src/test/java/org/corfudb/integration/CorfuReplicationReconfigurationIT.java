package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This suite of tests validates the behavior of Log Replication
 * when nodes are shutdown and brought back up.
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuReplicationReconfigurationIT extends LogReplicationAbstractIT {

    private static final int SLEEP_DURATION = 5;

    private AtomicBoolean replicationEnded = new AtomicBoolean(false);

    /**
     * Sets the plugin path before starting any test
     *
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
     * Test the case where Standby leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testStandbyClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO();

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Stop Standby Log Replicator Server
        log.debug(">>> (2) Stop Standby Node");
        stopStandbyLogReplicator();

        // (3) Start daemon thread writing data to active
        log.debug(">>> (3) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (4) Sleep Interval so writes keep going through, while standby is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Standby Log Replicator
        log.debug(">>> (5) Restart Standby Node");
        startStandbyLogReplicator();

        // (6) Verify Data on Standby after Restart
        log.debug(">>> (6) Verify Data on Standby");
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }

    /**
     * Test the case where Active leader node is restarted during log entry sync.
     *
     * The expectation is that replication should resume.
     */
    @Test
    public void testActiveClusterReset() throws Exception {
        // (1) Snapshot and Log Entry Sync
        log.debug(">>> (1) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySyncUFO();

        ExecutorService writerService = Executors.newSingleThreadExecutor();

        // (2) Start daemon thread writing data to active
        log.debug(">>> (2) Start daemon writer service");
        // Since step (1) wrote numWrites for snapshotSync and numWrites/2 in logEntrySync, continue from this starting point
        writerService.submit(() -> writeToActive((numWrites + numWrites/2), numWrites));

        // (3) Stop Active Log Replicator Server
        log.debug(">>> (3) Stop Active Node");
        stopActiveLogReplicator();

        // (4) Sleep Interval so writes keep going through, while standby is down
        log.debug(">>> (4) Wait for some time");
        Sleep.sleepUninterruptibly(Duration.ofSeconds(SLEEP_DURATION));

        // (5) Restart Active Log Replicator
        log.debug(">>> (5) Restart Active Node");
        startActiveLogReplicator();

        // (6) Verify Data on Standby after Restart
        log.debug(">>> (6) Verify Data on Standby");
        verifyDataOnStandby((numWrites*2 + numWrites/2));
    }

    @Test
    public void testSnapshotSyncApplyInterrupted() throws Exception {
        final int totalNumMaps = 5;
        final int standbyIndex = 2;
        final int numWritesSmaller = 1000;

        try {
            log.debug("Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            log.debug("Open map on active and standby");
            openMaps(totalNumMaps);

            // Subscribe to standby map 'Table002' (standbyIndex) to stop Standby LR as soon as updates are received,
            // forcing snapshot sync apply to be interrupted and resumed after LR standby is restarted
            subscribe(TABLE_PREFIX + standbyIndex);

            log.debug("Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWritesSmaller);

            // Confirm data does exist on Active Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
                assertThat(map.count()).isEqualTo(numWritesSmaller);
            }

            // Confirm data does not exist on Standby Cluster
            for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
                assertThat(map.count()).isEqualTo(0);
            }

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandby(numWritesSmaller);

            // Add Delta's for Log Entry Sync
            writeToActive(numWritesSmaller, numWritesSmaller / 2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyDataOnStandby((numWritesSmaller + (numWritesSmaller / 2)));
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
     * Validate that no data is written into the Standby's Transaction Log during replication
     * of 5K objects.
     *
     * @throws Exception
     */
    @Test
    public void testStandbyTransactionLogging() throws Exception {
        final long timeout = 30;

        replicationEnded.set(false);

        // (1) Subscribe Client to Standby Transaction Log
        log.debug(">>> (1) Subscribe to Transaction Stream on Standby");
        Future<Boolean> consumerState = subscribeTransactionStream();

        // (2) Snapshot and Log Entry Sync
        log.debug(">>> (2) Start Snapshot and Log Entry Sync");
        testEndToEndSnapshotAndLogEntrySync();

        replicationEnded.set(true);

        Boolean txStreamNotEmpty = consumerState.get(timeout, TimeUnit.SECONDS);
        assertThat(txStreamNotEmpty).isTrue();
    }

    private Future<Boolean> subscribeTransactionStream() {

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        List<CorfuRuntime> consumerRts = new ArrayList<>();

        // A thread that starts and consumes transaction updates via the Transaction Stream.
        return consumer.submit(() -> {

            CorfuRuntime consumerRt = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build())
                    .parseConfigurationString(standbyEndpoint)
                    .connect();

            consumerRts.add(consumerRt);

            IStreamView txStream = consumerRt.getStreamsView().get(ObjectsView.LOG_REPLICATOR_STREAM_ID);

            int counter = 0;

            // Stop polling only when all updates (from all writers) have
            // been consumed.
            while (!replicationEnded.get()) {
                List<ILogData> entries = txStream.remaining();

                if (!entries.isEmpty()) {
                    log.error("Transaction Log Entry Found. Entries={}", entries);
                    counter++;
                }
            }

            System.out.println("Total Transaction Stream updates, count=" + counter);
            log.info("Total Tx Stream updates = {}", counter);

            // We should have Txn Stream Updates
            return counter != 0;
        });
    }

    private void subscribe(String mapName) {
        // Subscribe to mapName and upon changes stop standby LR
        CorfuStore corfuStore = new CorfuStore(standbyRuntime);

        try {
            corfuStore.openTable(
                    NAMESPACE, mapName,
                    Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.builder().build()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StandbyMapListener configStreamListener = new StandbyMapListener(this);
        corfuStore.subscribeListener(configStreamListener, NAMESPACE, "test");
    }

    /**
     * Stream Listener on topology config table, which is for test only.
     * It enables ITs run as processes and communicate with the cluster manager
     * to update topology config.
     **/
    public static class StandbyMapListener implements StreamListener {

        private final LogReplicationAbstractIT abstractIT;

        private boolean interruptedSnapshotSyncApply = false;

        public StandbyMapListener(LogReplicationAbstractIT abstractIT) {
            this.abstractIT = abstractIT;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("StandbyMapListener:: onNext {} with entry size {}", results, results.getEntries().size());

            if (!interruptedSnapshotSyncApply) {

                interruptedSnapshotSyncApply = true;

                // Stop Log Replication Server so Snapshot Sync Apply is interrupted in the middle and restart
                log.debug("StandbyMapListener:: Stop Standby LR while in snapshot sync apply phase...");
                this.abstractIT.stopStandbyLogReplicator();

                log.debug("StandbyMapListener:: Restart Standby LR...");
                this.abstractIT.startStandbyLogReplicator();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // Ignore
        }
    }
}
