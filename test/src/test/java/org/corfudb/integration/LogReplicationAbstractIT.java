package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.SnapshotSyncPluginValue;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE;
import static org.junit.Assert.fail;

@Slf4j
public class LogReplicationAbstractIT extends AbstractIT {

    private static final int MSG_SIZE = 65536;

    private static final int SHORT_SLEEP_TIME = 10;

    public static final String TABLE_PREFIX = "Table00";

    public static final String NAMESPACE = "LR-Test";

    public static final String TAG_ONE = "tag_one";

    public final static String nettyConfig = "src/test/resources/transport/nettyConfig.properties";

    public String pluginConfigFilePath;

    // Note: this flag is kept for debugging purposes only.
    // Log Replication Server should run as a process as the unexpected termination of it
    // (for instance the completion of a test) causes SYSTEM_EXIT(ERROR_CODE).
    // If flipped to debug (easily access logs within the IDE, flip back before pushing upstream).
    public static boolean runProcess = true;

    public ExecutorService executorService = Executors.newFixedThreadPool(2);
    public Process activeCorfu = null;
    public Process standbyCorfu = null;

    public Process activeReplicationServer = null;
    public Process standbyReplicationServer = null;

    public final String streamA = "Table001";

    public final int numWrites = 2000;
    public final int lockLeaseDuration = 10;

    public final int activeSiteCorfuPort = 9000;
    public final int standbySiteCorfuPort = 9001;

    public final int activeReplicationServerPort = 9010;
    public final int standbyReplicationServerPort = 9020;

    public final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    public final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    public CorfuRuntime activeRuntime;
    public CorfuRuntime standbyRuntime;


    public Table<Sample.StringKey, Sample.IntValue, Sample.Metadata> mapA;
    public Table<Sample.StringKey, Sample.IntValue, Sample.Metadata> mapAStandby;

    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapActive;
    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapStandby;

    public CorfuStore corfuStoreActive;
    public CorfuStore corfuStoreStandby;

    private final long longInterval = 20L;

    public void testEndToEndSnapshotAndLogEntrySync() throws Exception {
        try {
            log.debug("Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            log.debug("Open map on active and standby");
            openMap();

            log.debug("Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActiveNonUFO(0, numWrites);

            // Confirm data does exist on Active Cluster
            assertThat(mapA.count()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Cluster
            assertThat(mapAStandby.count()).isEqualTo(0);

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandbyNonUFO(numWrites);

            // Add Delta's for Log Entry Sync
            writeToActiveNonUFO(numWrites, numWrites/2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyDataOnStandbyNonUFO((numWrites + (numWrites / 2)));
        } finally {
            tearDown();
        }

    }

    public void testEndToEndSnapshotAndLogEntrySyncUFO(boolean diskBased, boolean checkRemainingEntriesOnSecondLogEntrySync) throws Exception {
        testEndToEndSnapshotAndLogEntrySyncUFO(1, diskBased, checkRemainingEntriesOnSecondLogEntrySync);
    }

    public void testEndToEndSnapshotAndLogEntrySyncUFO(int totalNumMaps, boolean diskBased, boolean checkRemainingEntriesOnSecondLogEntrySync) throws Exception {
        // For the purpose of this test, standby should only update status 2 times:
        // (1) When starting snapshot sync apply : is_data_consistent = false
        // (2) When completing snapshot sync apply : is_data_consistent = true
        final int totalStandbyStatusUpdates = 2;

        try {
            log.info(">> Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            // Two updates are expected onStart of snapshot sync and onEnd.
            CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
            SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
            subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

            // Subscribe to replication status table on Standby (to be sure data change on status are captured)
            corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

            CountDownLatch statusUpdateLatch = new CountDownLatch(totalStandbyStatusUpdates);
            ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
            corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

            log.info(">> Open map(s) on active and standby");
            openMaps(totalNumMaps, diskBased);

            log.info(">> Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWrites);

            // Confirm data does exist on Active Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
                assertThat(map.count()).isEqualTo(numWrites);
            }

            // Confirm data does not exist on Standby Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
                assertThat(map.count()).isZero();
            }

            startLogReplicatorServers();

            log.info(">> Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandby(numWrites);

            // Confirm replication status reflects snapshot sync completed
            log.info(">> Verify replication status completed on active");
            verifyReplicationStatusFromActive();

            // Wait until both plugin updates
            log.info(">> Wait snapshot sync plugin updates received");
            latchSnapshotSyncPlugin.await();
            // Confirm snapshot sync plugin was triggered on start and on end
            validateSnapshotSyncPlugin(snapshotSyncPluginListener);

            // Add Delta's for Log Entry Sync
            log.info(">> Write deltas");
            writeToActive(numWrites, numWrites / 2);

            log.info(">> Wait ... Delta log replication in progress ...");
            verifyDataOnStandby((numWrites + (numWrites / 2)));

            // Verify Standby Status Listener received all expected updates (is_data_consistent)
            log.info(">> Wait ... Replication status UPDATE ...");
            statusUpdateLatch.await();
            assertThat(standbyListener.getAccumulatedStatus().size()).isEqualTo(totalStandbyStatusUpdates);
            // Confirm last updates are set to true (corresponding to snapshot sync completed and log entry sync started)
            assertThat(standbyListener.getAccumulatedStatus().get(standbyListener.getAccumulatedStatus().size() - 1)).isTrue();
            assertThat(standbyListener.getAccumulatedStatus()).contains(false);

            if (checkRemainingEntriesOnSecondLogEntrySync) {
                triggerSnapshotAndTestRemainingEntries();
            }

        } finally {
            tearDown();
        }
    }

    private void triggerSnapshotAndTestRemainingEntries() throws Exception{

        // enforce a snapshot sync
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                corfuStoreActive.openTable(
                        DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                        ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                        TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
                );
        try (TxnContext txn = corfuStoreActive.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                    DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
            txn.commit();
        }

        // Sleep, so we have remainingEntries populated from the scheduled polling task instead of
        // from method that marks snapshot complete
        TimeUnit.SECONDS.sleep(longInterval);

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        ReplicationStatusListener activeListener = new ReplicationStatusListener(statusUpdateLatch, true);
        corfuStoreActive.subscribeListener(activeListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();
        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;

        statusUpdateLatch.await();
        try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (LogReplicationMetadata.ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
            txn.commit();
        }

        assertThat(replicationStatusVal.getRemainingEntriesToSend()).isEqualTo(0);
    }

    /**
     * This test verifies that the eventListener is resubscribed correctly when the streaming layer encounters an error.
     * 1. Validate that the setup created is stable by validating the snapshot_sync data
     * 2. Add a valid force snapshot_sync event in the event Table.
     * 3. Ensure that the snapshot sync was completed for the event and the event is effectively removed once processed
     * 4. Add an invalid record to the event table. This makes the eventListener in LR stall by repeatedly trying to
     * read the invalid record and encountering an error.
     * While LR is in that state, delete theinvalid event and run the CP/trim twice. This results in the listener
     * hitting the Trimmed Exception.
     * 5. Write a valid force snapshot_sync event in the event Table.
     * A successful snapshot sync indicates that the eventListener was able to resubscribe to the eventTable even after encountering few errors.
     */
    protected void testEventListenerEndToEnd(int totalNumMaps) throws Exception {
        // For the purpose of this test, standby should only update status 2 times:
        // (1) When starting snapshot sync apply : is_data_consistent = false
        // (2) When completing snapshot sync apply : is_data_consistent = true
        final int totalStandbyStatusUpdates = 2;

        try {
            log.info(">> Setup active and standby Corfu's");
            setupActiveAndStandbyCorfu();

            // Subscribe to replication status table on Standby (to be sure data change on status are captured)
            corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

            Table<LogReplicationMetadata.ReplicationEventKey, LogReplicationMetadata.ReplicationEvent, Message> replicationEventTable =
                    corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_EVENT_TABLE_NAME,
                    LogReplicationMetadata.ReplicationEventKey.class,
                    LogReplicationMetadata.ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationEvent.class));

            CountDownLatch statusUpdateLatch = new CountDownLatch(totalStandbyStatusUpdates);
            ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch, false);
            corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

            log.info(">> Open map(s) on active and standby");
            openMaps(totalNumMaps, false);

            log.info(">> Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWrites);

            // Confirm data does exist on Active Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
                assertThat(map.count()).isEqualTo(numWrites);
            }

            // Confirm data does not exist on Standby Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
                assertThat(map.count()).isZero();
            }

            startLogReplicatorServers();

            // Verify Standby Status Listener received all expected updates (is_data_consistent)
            log.info(">> Wait ... Replication status UPDATE ...");
            statusUpdateLatch.await();

            verifyDataOnStandby(numWrites);

            // Confirm replication status reflects snapshot sync completed
            log.info(">> Verify replication status completed on active");
            verifyReplicationStatusFromActive();

            verifySnapshotSync(standbyListener, totalStandbyStatusUpdates);

            //reset the listener
            standbyListener.reset(new CountDownLatch(totalStandbyStatusUpdates));


            LogReplicationMetadata.ReplicationEventKey key = LogReplicationMetadata.ReplicationEventKey
                    .newBuilder().setKey(UUID.randomUUID().toString()).build();
            LogReplicationMetadata.ReplicationEvent event = LogReplicationMetadata.ReplicationEvent.newBuilder()
                    .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                    .setEventId(UUID.randomUUID().toString())
                    .setType(LogReplicationMetadata.ReplicationEvent.ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                    .build();

            // add a valid snapshot sync event in the event table
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                txn.putRecord(replicationEventTable, key, event, null);
                txn.commit();
            }

            standbyListener.countDownLatch.await();
            verifySnapshotSync(standbyListener, totalStandbyStatusUpdates);

            while(replicationEventTable.count() != 0) {
                //wait
            }
            // verify the event table is cleared after an event is processed
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                assertThat(txn.getTable(REPLICATION_EVENT_TABLE_NAME).count()).isZero();
            }

            //reset the listener
            standbyListener.reset(new CountDownLatch(totalStandbyStatusUpdates));

            // Add an invalid entry to the table so the event listener's onError() gets called in a loop, giving the test to CP/trim
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                txn.putRecord(replicationEventTable, key, LogReplicationMetadata.ReplicationEvent.newBuilder().build(), null);
                txn.commit();
            }

            // delete the invalid entry, so the full sync upon resubscription doesn't see the invalid record
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                txn.delete(replicationEventTable, key);
                txn.commit();
            }

            // run CP twice so the listener hits a trimmed exception
            checkpointAndTrim(true);
            checkpointAndTrim(true);

            // add a valid entry and ensure that the listener process it
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                txn.putRecord(replicationEventTable, key, event, null);
                txn.commit();
            }
            standbyListener.countDownLatch.await();
            verifySnapshotSync(standbyListener, totalStandbyStatusUpdates);


        } finally {
            tearDown();
        }
    }

    private void verifySnapshotSync(ReplicationStatusListener standbyListener, int totalStandbyStatusUpdates) {
        assertThat(standbyListener.getAccumulatedStatus().size()).isEqualTo(totalStandbyStatusUpdates);
        // Confirm last updates are set to true (corresponding to snapshot sync completed and log entry sync started)
        assertThat(standbyListener.getAccumulatedStatus().get(standbyListener.getAccumulatedStatus().size() - 1)).isTrue();
        assertThat(standbyListener.getAccumulatedStatus()).contains(false);
    }

    private void tearDown() {
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

    private void verifyReplicationStatusFromActive() throws Exception {
        Table<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, LogReplicationMetadata.ReplicationStatusVal>
                replicationStatusTable = corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        IRetry.build(IntervalRetry.class, () -> {
            try(TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                List<CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, LogReplicationMetadata.ReplicationStatusVal>>
                        entries = txn.executeQuery(replicationStatusTable, all -> true);
                assertThat(entries.size()).isNotZero();
                if (entries.get(0).getPayload().getSnapshotSyncInfo().getStatus() != LogReplicationMetadata.SyncStatus.COMPLETED) {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(SHORT_SLEEP_TIME));
                    txn.commit();
                    throw new RetryNeededException();
                }
                assertThat(entries.get(0).getPayload().getSnapshotSyncInfo().getStatus()).isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
                assertThat(entries.get(0).getPayload().getSnapshotSyncInfo().getType()).isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
                assertThat(entries.get(0).getPayload().getSnapshotSyncInfo().getBaseSnapshot()).isNotEqualTo(Address.NON_ADDRESS);
                txn.commit();
            } catch (TransactionAbortedException tae) {
                log.error("Error while attempting to connect to update dummy table in onSnapshotSyncStart.", tae);
                throw new RetryNeededException();
            }
            return null;
        }).run();
    }

    void validateSnapshotSyncPlugin(SnapshotSyncPluginListener listener) {
        assertThat(listener.getUpdates().size()).isEqualTo(2);
        assertThat(listener.getUpdates()).contains(DefaultSnapshotSyncPlugin.ON_START_VALUE);
        assertThat(listener.getUpdates()).contains(DefaultSnapshotSyncPlugin.ON_END_VALUE);
    }

    void subscribeToSnapshotSyncPluginTable(SnapshotSyncPluginListener listener) {
        try {
            corfuStoreStandby.openTable(DefaultSnapshotSyncPlugin.NAMESPACE,
                    DefaultSnapshotSyncPlugin.TABLE_NAME, ExampleSchemas.Uuid.class, SnapshotSyncPluginValue.class,
                    SnapshotSyncPluginValue.class, TableOptions.fromProtoSchema(SnapshotSyncPluginValue.class));
            corfuStoreStandby.subscribeListener(listener, DefaultSnapshotSyncPlugin.NAMESPACE, DefaultSnapshotSyncPlugin.TAG);
        } catch (Exception e) {
            fail("Exception while attempting to subscribe to snapshot sync plugin table");
        }
    }

    class SnapshotSyncPluginListener implements StreamListener {

        @Getter
        Set<String> updates = new HashSet<>();

        private final CountDownLatch countDownLatch;

        public SnapshotSyncPluginListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e ->
                    updates.add(((SnapshotSyncPluginValue)e.getPayload()).getValue())));
            countDownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for SnapshotSyncPluginListener");
        }
    }

    class ReplicationStatusListener implements StreamListener {

        @Getter
        List<Boolean> accumulatedStatus = new ArrayList<>();

        private CountDownLatch countDownLatch;
        private boolean waitSnapshotStatusComplete;

        public ReplicationStatusListener(CountDownLatch countdownLatch, boolean waitSnapshotStatusComplete) {
            this.countDownLatch = countdownLatch;
            this.waitSnapshotStatusComplete = waitSnapshotStatusComplete;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                    LogReplicationMetadata.ReplicationStatusVal statusVal = (LogReplicationMetadata.ReplicationStatusVal)e.getPayload();
                    accumulatedStatus.add(statusVal.getDataConsistent());
                    if (this.waitSnapshotStatusComplete && statusVal.getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
                        countDownLatch.countDown();
                    }
            }));

            if (!this.waitSnapshotStatusComplete) {
                countDownLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for ReplicationStatusListener : " + throwable.toString());
        }

        private void reset(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
            accumulatedStatus.clear();
        }
    }

    public void setupActiveAndStandbyCorfu() throws Exception {
        try {
            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            // Setup runtime's to active and standby Corfu
            activeRuntime = createRuntimeWithCache(activeEndpoint);
            standbyRuntime = createRuntimeWithCache(standbyEndpoint);

            corfuStoreActive = new CorfuStore(activeRuntime);
            corfuStoreStandby = new CorfuStore(standbyRuntime);
        } catch (Exception e) {
            log.debug("Error while starting Corfu");
            throw  e;
        }
    }

    public void openMaps(int mapCount, boolean diskBased) throws Exception {
        mapNameToMapActive = new HashMap<>();
        mapNameToMapStandby = new HashMap<>();
        Path pathActive = null;
        Path pathStandby = null;

        for(int i=1; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            if (diskBased) {
                pathActive = Paths.get(com.google.common.io.Files.createTempDir().getAbsolutePath());
                pathStandby = Paths.get(com.google.common.io.Files.createTempDir().getAbsolutePath());
            }

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapActive = corfuStoreActive.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class, TableOptions.builder().persistentDataPath(pathActive).build()));

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class, TableOptions.builder().persistentDataPath(pathStandby).build()));

            mapNameToMapActive.put(mapName, mapActive);
            mapNameToMapStandby.put(mapName, mapStandby);

            assertThat(mapActive.count()).isZero();
            assertThat(mapStandby.count()).isZero();
        }
    }

    public void openMap() throws Exception {
        // Write to StreamA on Source side
        mapA = corfuStoreActive.openTable(
                NAMESPACE,
                streamA,
                Sample.StringKey.class,
                Sample.IntValue.class,
                Sample.Metadata.class,
                TableOptions.builder().schemaOptions(
                                CorfuOptions.SchemaOptions.newBuilder()
                                        .setIsFederated(true)
                                        .addStreamTag(ObjectsView.LOG_REPLICATOR_STREAM_INFO.getTagName())
                                        .build())
                        .build()
        );

        mapAStandby = corfuStoreStandby.openTable(
                NAMESPACE,
                streamA,
                Sample.StringKey.class,
                Sample.IntValue.class,
                Sample.Metadata.class,
                TableOptions.builder().schemaOptions(
                                CorfuOptions.SchemaOptions.newBuilder()
                                        .setIsFederated(true)
                                        .addStreamTag(ObjectsView.LOG_REPLICATOR_STREAM_INFO.getTagName())
                                        .build())
                        .build()
        );

        assertThat(mapA.count()).isEqualTo(0);
        assertThat(mapAStandby.count()).isEqualTo(0);
    }

    public void writeToActiveNonUFO(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                txn.putRecord(mapA, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        Sample.IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
    }

    public void writeToActive(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for(Map.Entry<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> entry : mapNameToMapActive.entrySet()) {

            log.debug(">>> Write to active cluster, map={}", entry.getKey());

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map = entry.getValue();
            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                Sample.IntValueTag IntValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreActive.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, IntValueTag, metadata);
                    txn.commit();
                }
            }
        }
    }

    public void startLogReplicatorServers() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer =
                    runReplicationServer(activeReplicationServerPort, pluginConfigFilePath,
                        lockLeaseDuration);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer =
                    runReplicationServer(standbyReplicationServerPort, pluginConfigFilePath,
                        lockLeaseDuration);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--max-replication-data-message-size=" + MSG_SIZE,  "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--lock-lease=5", String.valueOf(activeReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--max-replication-data-message-size=" + MSG_SIZE, "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--lock-lease=5", String.valueOf(standbyReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void stopActiveLogReplicator() {
        if(runProcess) {
            stopLogReplicator(true);
        }
    }

    public void stopStandbyLogReplicator() {
        if (runProcess) {
            stopLogReplicator(false);
        }
    }

    private void stopLogReplicator(boolean active) {
        int port = active ? activeReplicationServerPort : standbyReplicationServerPort;
        List<String> paramsPs = Arrays.asList("/bin/sh", "-c", "ps aux | grep CorfuInterClusterReplicationServer | grep " + port);
        String result = runCommandForOutput(paramsPs);

        // Get PID
        String[] output = result.split(" ");
        int i = 0;
        String pid = "";
        for (String st : output) {
            if (!st.equals("")) {
                i++;
                if (i == 2) {
                    pid = st;
                    break;
                }
            }
        }

        log.debug("*** Active PID :: " + pid);

        List<String> paramsKill = Arrays.asList("/bin/sh", "-c", "kill -9 " + pid);
        runCommandForOutput(paramsKill);

        if (active && activeReplicationServer != null) {
            activeReplicationServer.destroyForcibly();
            activeReplicationServer = null;
        } else if (!active && standbyReplicationServer != null) {
            standbyReplicationServer.destroyForcibly();
            standbyReplicationServer = null;
        }
    }

    public static String runCommandForOutput(List<String> params) {
        ProcessBuilder pb = new ProcessBuilder(params);
        Process p;
        String result = "";
        try {
            p = pb.start();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
            reader.lines().iterator().forEachRemaining(sj::add);
            result = sj.toString();

            p.waitFor();
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void startActiveLogReplicator() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, pluginConfigFilePath, lockLeaseDuration);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", String.valueOf(activeReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void startStandbyLogReplicator() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Active Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, pluginConfigFilePath, lockLeaseDuration);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", String.valueOf(standbyReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void verifyDataOnStandbyNonUFO(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapAStandby.count() != expectedConsecutiveWrites) {
            // Block until expected number of entries is reached
            log.trace("Map size: {}, expect size: {}",
                    mapAStandby.count(), expectedConsecutiveWrites);
        }

        log.debug("Number updates on Standby :: " + expectedConsecutiveWrites);

        // Verify data is present in Standby Site
        assertThat(mapAStandby.count()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            try (TxnContext tx = corfuStoreStandby.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapAStandby, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
    }

    public void verifyDataOnStandby(int expectedConsecutiveWrites) {
        for(Map.Entry<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> entry : mapNameToMapStandby.entrySet()) {

            log.info("Verify Data on Standby's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.info("Number updates on Standby Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Standby Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreStandby.txn(entry.getValue().getNamespace())) {
                    assertThat(tx.getRecord(entry.getValue(),
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }

    public void verifyInLogEntrySyncState() throws InterruptedException {
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(DefaultClusterConfig.getStandbyClusterId())
                        .build();

        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal = null;

        while (replicationStatusVal == null || !replicationStatusVal.getSyncType().equals(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY)
                || !replicationStatusVal.getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
            TimeUnit.SECONDS.sleep(1);
            try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                replicationStatusVal = (LogReplicationMetadata.ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
    }

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param active true, checkpoint/trim on active cluster
     *               false, checkpoint/trim on standby cluster
     */
    public void checkpointAndTrim(boolean active) {
        CorfuRuntime cpRuntime;

        if (active) {
            cpRuntime = createRuntimeWithCache(activeEndpoint);
        } else {
            cpRuntime = createRuntimeWithCache(standbyEndpoint);
        }

        checkpointAndTrimCorfuStore(cpRuntime);
    }

    public static Token checkpointAndTrimCorfuStore(CorfuRuntime cpRuntime) {
        // Open Table Registry
        TableRegistry tableRegistry = cpRuntime.getTableRegistry();
        PersistentCorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                CorfuStoreMetadata.TableMetadata>> tableRegistryCT = tableRegistry.getRegistryTable();

        PersistentCorfuTable<CorfuStoreMetadata.ProtobufFileName,
                    CorfuRecord<CorfuStoreMetadata.ProtobufFileDescriptor, CorfuStoreMetadata.TableMetadata>>
            protobufDescriptorTable =
            tableRegistry.getProtobufDescriptorTable();

        // Save the regular serializer first..
        ISerializer protoBufSerializer = cpRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtoBufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtoBufSerializer
        ISerializer dynamicProtoBufSerializer = new DynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(dynamicProtoBufSerializer);

        // First checkpoint the TableRegistry system table
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        Token trimMark = null;

        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            // ProtobufDescriptor table is an internal table which must not
            // be checkpointed using the DynamicProtobufSerializer
            if (tableName.getTableName().equals(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                continue;
            }
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                    tableName.getNamespace(), tableName.getTableName()
            );

            PersistentCorfuTable<CorfuDynamicKey, CorfuDynamicRecord> corfuTable =
                    createCorfuTable(cpRuntime, fullTableName, dynamicProtoBufSerializer);

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTable);

            Token token = mcw.appendCheckpoints(cpRuntime, "checkpointer");
            trimMark = trimMark == null ? token : Token.min(trimMark, token);
        }

        // Finally checkpoint the ProtobufDescriptor and TableRegistry system
        // tables
        // Restore the regular protoBuf serializer and undo the dynamic
        // protoBuf serializer
        // otherwise the test cannot continue beyond this point.
        log.info("Now checkpointing the ProtobufDescriptor and Registry " +
            "Tables");
        cpRuntime.getSerializers().registerSerializer(protoBufSerializer);
        mcw.addMap(protobufDescriptorTable);
        Token token1 = mcw.appendCheckpoints(cpRuntime, "checkpointer");

        mcw.addMap(tableRegistryCT);
        Token token2 = mcw.appendCheckpoints(cpRuntime, "checkpointer");
        Token minToken = Token.min(token1, token2);
        trimMark = trimMark != null ? Token.min(trimMark, minToken) : minToken;

        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().gc();

        // Trim
        log.debug("**** Trim Log @address=" + trimMark);
        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().invalidateClientCache();
        cpRuntime.getAddressSpaceView().invalidateServerCaches();
        cpRuntime.getAddressSpaceView().gc();

        return trimMark;
    }

    /**
     * Stream Listener used for testing streaming on standby site. This listener decreases a latch
     * until all expected updates are received/
     */
    public static class StreamingStandbyListener implements StreamListener {

        private final CountDownLatch updatesLatch;
        private final CountDownLatch numTxLatch;
        public List<CorfuStreamEntry> messages = new ArrayList<>();
        private final Set<UUID> tablesToListenTo;

        public StreamingStandbyListener(CountDownLatch updatesLatch, CountDownLatch numTxLatch, Set<UUID> tablesToListenTo) {
            this.updatesLatch = updatesLatch;
            this.tablesToListenTo = tablesToListenTo;
            this.numTxLatch = numTxLatch;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("StreamingStandbyListener:: onNext {} with entry size {}", results, results.getEntries().size());
            numTxLatch.countDown();

            results.getEntries().forEach((schema, entries) -> {
                if (tablesToListenTo.contains(CorfuRuntime.getStreamID(NAMESPACE + "$" + schema.getTableName()))) {
                    messages.addAll(entries);
                    entries.forEach(e -> {
                        if (e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                            System.out.println("Clear operation for :: " + schema.getTableName() + " on address :: "
                                    + results.getTimestamp().getSequence() + " key :: " + e.getKey());
                        }
                        updatesLatch.countDown();
                    });
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("ERROR :: unsubscribed listener");
        }
    }
}
