package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

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

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.SnapshotSyncInfo;
import org.corfudb.runtime.LogReplication.SnapshotSyncInfo.SnapshotSyncType;
import org.corfudb.runtime.LogReplication.SyncType;
import org.corfudb.runtime.LogReplication.SyncStatus;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
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
import org.corfudb.runtime.LogReplication.LogReplicationSession;
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

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;

@Slf4j
public class LogReplicationAbstractIT extends AbstractIT {

    private static final int MSG_SIZE = 65536;

    private static final int SHORT_SLEEP_TIME = 10;

    public static final String TABLE_PREFIX = "Table00";

    public static final String NAMESPACE = "LR-Test";

    public static final String TAG_ONE = "tag_one";

    public static String pluginConfigFilePath = "src/test/resources/transport/pluginConfig.properties";

    // Note: this flag is kept for debugging purposes only.
    // Log Replication Server should run as a process as the unexpected termination of it
    // (for instance the completion of a test) causes SYSTEM_EXIT(ERROR_CODE).
    // If flipped to debug (easily access logs within the IDE, flip back before pushing upstream).
    public static boolean runProcess = true;

    public ExecutorService executorService = Executors.newFixedThreadPool(2);
    public Process sourceCorfu = null;
    public Process sinkCorfu = null;

    public Process sourceReplicationServer = null;
    public Process sinkReplicationServer = null;

    public final String streamA = "Table001";

    public final int numWrites = 2000;
    public final int lockLeaseDuration = 10;

    public final int sourceSiteCorfuPort = 9000;
    public final int sinkSiteCorfuPort = 9001;

    public final int sourceReplicationServerPort = 9010;
    public final int sinkReplicationServerPort = 9020;

    public final String sourceEndpoint = DEFAULT_HOST + ":" + sourceSiteCorfuPort;
    public final String sinkEndpoint = DEFAULT_HOST + ":" + sinkSiteCorfuPort;

    public CorfuRuntime sourceRuntime;
    public CorfuRuntime sinkRuntime;

    public Table<Sample.StringKey, Sample.IntValue, Sample.Metadata> mapA;
    public Table<Sample.StringKey, Sample.IntValue, Sample.Metadata> mapASink;

    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapSource;
    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapSink;

    public CorfuStore corfuStoreSource;
    public CorfuStore corfuStoreSink;

    private final long longInterval = 20L;

    // default is single Source-Sink topology
    public ExampleSchemas.ClusterUuidMsg topologyType = DefaultClusterManager.TP_SINGLE_SOURCE_SINK;

    public String transportType = "GRPC";

    public void testEndToEndSnapshotAndLogEntrySync() throws Exception {
        try {
            log.debug("Setup source and sink Corfu's");
            setupSourceAndSinkCorfu();
            initSingleSourceSinkCluster();

            log.debug("Open map on source and sink");
            openMap();

            log.debug("Write data to source CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToSourceNonUFO(0, numWrites);

            // Confirm data does exist on Source Cluster
            assertThat(mapA.count()).isEqualTo(numWrites);

            // Confirm data does not exist on Sink Cluster
            assertThat(mapASink.count()).isEqualTo(0);

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnSinkNonUFO(numWrites);

            // Add Delta's for Log Entry Sync
            writeToSourceNonUFO(numWrites, numWrites/2);

            log.debug("Wait ... Delta log replication in progress ...");

            verifyDataOnSinkNonUFO(numWrites + (numWrites / 2));
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

    public void testEndToEndSnapshotAndLogEntrySyncUFO(boolean diskBased,
                                                       boolean checkRemainingEntriesOnSecondLogEntrySync,
                                                       int numSourceClusters, boolean cleanup) throws Exception {
        testEndToEndSnapshotAndLogEntrySyncUFO(1, diskBased, checkRemainingEntriesOnSecondLogEntrySync,
                numSourceClusters, cleanup);
    }

    public void testEndToEndSnapshotAndLogEntrySyncUFO(int totalNumMaps, boolean diskBased,
                                                       boolean checkRemainingEntriesOnSecondLogEntrySync,
                                                       int numSourceClusters, boolean cleanup) throws Exception {
        // For the purpose of this test, Sink should get 3 transactions for status update:
        // (1) On startup, init the replication status for each Source cluster in a single transaction
        // (2) When starting snapshot sync apply : is_data_consistent = false
        // (3) When completing snapshot sync apply : is_data_consistent = true
        final int totalSinkStatusUpdateTx = 2;

        // Across the above 3 transactions, there will be 5 updates/entries:
        // 1 (1 init update corresponding to each Source cluster )   +      1 (is_data_consistent = false)    + 1
        // (is_data_consistent = true)
        final int totalSinkStatusUpdateEntries = numSourceClusters + 2;
        try {
            log.info(">> Setup source and sink Corfu's");
            setupSourceAndSinkCorfu();
            initSingleSourceSinkCluster();

            // Two updates are expected onStart of snapshot sync and onEnd.
            CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
            SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
            subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

            // Subscribe to replication status table on Sink (to be sure data change on status are captured)
            corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE, REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class, ReplicationStatus.class, null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

            CountDownLatch statusUpdateLatch = new CountDownLatch(totalSinkStatusUpdateTx);
            ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
            corfuStoreSink.subscribeListener(sinkListener, LogReplicationMetadataManager.NAMESPACE, LR_STATUS_STREAM_TAG);

            log.info(">> Open map(s) on source and sink");
            openMaps(totalNumMaps, diskBased);

            log.info(">> Write data to source CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToSource(0, numWrites);

            // Confirm data does exist on Source Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSource.values()) {
                assertThat(map.count()).isEqualTo(numWrites);
            }

            // Confirm data does not exist on Sink Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapSink.values()) {
                assertThat(map.count()).isZero();
            }

            startLogReplicatorServers();

            log.info(">> Wait ... Snapshot log replication in progress ...");
            verifyDataOnSink(numWrites);

            // Confirm replication status reflects snapshot sync completed
            log.info(">> Verify replication status completed on source");
            verifyReplicationStatusFromSource();

            // Wait until both plugin updates
            log.info(">> Wait snapshot sync plugin updates received");
            latchSnapshotSyncPlugin.await();
            // Confirm snapshot sync plugin was triggered on start and on end
            validateSnapshotSyncPlugin(snapshotSyncPluginListener);

            // Add Delta's for Log Entry Sync
            log.info(">> Write deltas");
            writeToSource(numWrites, numWrites / 2);

            log.info(">> Wait ... Delta log replication in progress ...");
            verifyDataOnSink(numWrites + (numWrites / 2));

            // Verify Sink Status Listener received all expected updates (is_data_consistent)
            log.info(">> Wait ... Replication status UPDATE ..." + statusUpdateLatch.getCount());
            statusUpdateLatch.await();
            assertThat(sinkListener.getAccumulatedStatus().size()).isEqualTo(totalSinkStatusUpdateEntries);
            // Confirm last updates are set to true (corresponding to snapshot sync completed and log entry sync started)
            assertThat(sinkListener.getAccumulatedStatus().get(sinkListener.getAccumulatedStatus().size() - 1)).isTrue();
            assertThat(sinkListener.getAccumulatedStatus()).contains(false);

            if (checkRemainingEntriesOnSecondLogEntrySync) {
                triggerSnapshotAndTestRemainingEntries();
            }

        } finally {
            executorService.shutdownNow();

            if(!cleanup) {
                return;
            }

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

    public void initSingleSourceSinkCluster() throws Exception {
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                corfuStoreSource.openTable(
                        DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                        ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                        TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
                );
        try (TxnContext txn = corfuStoreSource.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, this.topologyType, this.topologyType, this.topologyType);
            txn.commit();
        }
    }

    private void triggerSnapshotAndTestRemainingEntries() throws Exception{

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        ReplicationStatusListener sourceListener = new ReplicationStatusListener(statusUpdateLatch, true);
        corfuStoreSource.subscribeListener(sourceListener, LogReplicationMetadataManager.NAMESPACE, LR_STATUS_STREAM_TAG);

        corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class));

        // enforce a snapshot sync
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                corfuStoreSource.openTable(
                        DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                        ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                        TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
                );
        try (TxnContext txn = corfuStoreSource.txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC,
                    DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC, DefaultClusterManager.OP_ENFORCE_SNAPSHOT_FULL_SYNC);
            txn.commit();
        }

        // Sleep, so we have remainingEntries populated from the scheduled polling task instead of
        // from method that marks snapshot complete
        TimeUnit.SECONDS.sleep(longInterval);

        LogReplicationSession session = LogReplicationSession.newBuilder()
            .setSourceClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
            .setSinkClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
            .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
            .build();

        ReplicationStatus replicationStatus;
        statusUpdateLatch.await();

        try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatus = (ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE_NAME, session).getPayload();
            txn.commit();
        }

        assertThat(replicationStatus.getSourceStatus().getRemainingEntriesToSend()).isEqualTo(0);
    }

    private void verifyReplicationStatusFromSource() throws Exception {
        Table<LogReplicationSession, ReplicationStatus, Message> replicationStatusTable =
            corfuStoreSource.openTable(
                LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplicationSession.class,
                ReplicationStatus.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatus.class)
            );

        String sinkClusterId =
            new DefaultClusterConfig().getSinkClusterIds().get(0);

        String sourceClusterId = new DefaultClusterConfig().getSourceClusterIds().get(0);

        LogReplicationSession session = LogReplicationSession.newBuilder()
            .setSourceClusterId(sourceClusterId)
            .setSinkClusterId(sinkClusterId)
            .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
            .build();

        IRetry.build(IntervalRetry.class, () -> {
            try(TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
                CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry =
                    txn.getRecord(replicationStatusTable, session);

                if (entry.getPayload().getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus() !=
                    SyncStatus.COMPLETED) {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(SHORT_SLEEP_TIME));
                    txn.commit();
                    throw new RetryNeededException();
                }
                assertThat(entry.getPayload().getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                    .isEqualTo(SyncStatus.COMPLETED);
                assertThat(entry.getPayload().getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getType())
                    .isEqualTo(SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
                assertThat(entry.getPayload().getSourceStatus().getReplicationInfo().getSnapshotSyncInfo()
                    .getBaseSnapshot()).isNotEqualTo(Address.NON_ADDRESS);
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
            corfuStoreSink.openTable(DefaultSnapshotSyncPlugin.NAMESPACE,
                    DefaultSnapshotSyncPlugin.TABLE_NAME, ExampleSchemas.Uuid.class, SnapshotSyncPluginValue.class,
                    SnapshotSyncPluginValue.class, TableOptions.fromProtoSchema(SnapshotSyncPluginValue.class));
            corfuStoreSink.subscribeListener(listener, DefaultSnapshotSyncPlugin.NAMESPACE, DefaultSnapshotSyncPlugin.TAG);
        } catch (Exception e) {
            fail("Exception while attempting to subscribe to snapshot sync plugin table");
        }
    }

    static class SnapshotSyncPluginListener implements StreamListener {

        @Getter
        Set<String> updates = new HashSet<>();

        private final CountDownLatch countDownLatch;

        public SnapshotSyncPluginListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                    updates.add(((SnapshotSyncPluginValue)e.getPayload()).getValue());
            }));
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

                // Replication Status table gets cleared as part of the simulated upgrade in
                // CorfuReplicationUpgradeIT.  These updates must be ignored.
                if (e.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                    return;
                }
                ReplicationStatus status = (ReplicationStatus) e.getPayload();
                accumulatedStatus.add(status.getSinkStatus().getDataConsistent());
                if (this.waitSnapshotStatusComplete &&
                    status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                        .equals(SyncStatus.COMPLETED)) {
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
    }

    public void setupSourceAndSinkCorfu() throws Exception {
        try {
            // Start Single Corfu Node Cluster on Source Site
            sourceCorfu = runServer(sourceSiteCorfuPort, true);

            // Start Corfu Cluster on Sink Site
            sinkCorfu = runServer(sinkSiteCorfuPort, true);

            // Setup runtime's to source and sink Corfu
            sourceRuntime = createRuntimeWithCache(sourceEndpoint);
            sinkRuntime = createRuntimeWithCache(sinkEndpoint);

            corfuStoreSource = new CorfuStore(sourceRuntime);
            corfuStoreSink = new CorfuStore(sinkRuntime);
        } catch (Exception e) {
            log.debug("Error while starting Corfu");
            throw  e;
        }
    }

    public void openMaps(int mapCount, boolean diskBased) throws Exception {
        mapNameToMapSource = new HashMap<>();
        mapNameToMapSink = new HashMap<>();
        Path pathSource = null;
        Path pathSink = null;

        for(int i=1; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            if (diskBased) {
                pathSource = Paths.get(com.google.common.io.Files.createTempDir().getAbsolutePath());
                pathSink = Paths.get(com.google.common.io.Files.createTempDir().getAbsolutePath());
            }

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSource = corfuStoreSource.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class, TableOptions.builder().persistentDataPath(pathSource).build()));

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapSink = corfuStoreSink.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class, TableOptions.builder().persistentDataPath(pathSink).build()));

            mapNameToMapSource.put(mapName, mapSource);
            mapNameToMapSink.put(mapName, mapSink);

            assertThat(mapSource.count()).isZero();
            assertThat(mapSink.count()).isZero();
        }
    }

    public void openMap() throws Exception {
        // Write to StreamA on Source side
        mapA = corfuStoreSource.openTable(
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

        mapASink = corfuStoreSink.openTable(
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
        assertThat(mapASink.count()).isEqualTo(0);
    }

    public void writeToSourceNonUFO(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                txn.putRecord(mapA, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build(),
                        Sample.IntValue.newBuilder().setValue(i).build(), null);
                txn.commit();
            }
        }
    }

    public void writeToSource(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for(Map.Entry<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> entry : mapNameToMapSource.entrySet()) {

            log.debug(">>> Write to source cluster, map={}", entry.getKey());

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map = entry.getValue();
            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                Sample.IntValueTag IntValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                    txn.putRecord(map, stringKey, IntValueTag, metadata);
                    txn.commit();
                }
            }
        }
    }

    public void startLogReplicatorServers() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Source Site
                sourceReplicationServer =
                    runReplicationServer(sourceReplicationServerPort, sourceSiteCorfuPort, pluginConfigFilePath,
                        lockLeaseDuration, transportType);

                // Start Log Replication Server on Sink Site
                sinkReplicationServer =
                    runReplicationServer(sinkReplicationServerPort, sinkSiteCorfuPort, pluginConfigFilePath,
                        lockLeaseDuration, transportType);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--max-replication-data-message-size=" + MSG_SIZE,  "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--lock-lease=5", String.valueOf(sourceReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--max-replication-data-message-size=" + MSG_SIZE, "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--lock-lease=5", String.valueOf(sinkReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void stopSourceLogReplicator() {
        if(runProcess) {
            stopLogReplicator(true);
        }
    }

    public void stopSinkLogReplicator() {
        if (runProcess) {
            stopLogReplicator(false);
        }
    }

    private void stopLogReplicator(boolean source) {
        int port = source ? sourceReplicationServerPort : sinkReplicationServerPort;
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

        log.debug("*** Source PID :: " + pid);

        List<String> paramsKill = Arrays.asList("/bin/sh", "-c", "kill -9 " + pid);
        runCommandForOutput(paramsKill);

        if (source && sourceReplicationServer != null) {
            sourceReplicationServer.destroyForcibly();
            sourceReplicationServer = null;
        } else if (!source && sinkReplicationServer != null) {
            sinkReplicationServer.destroyForcibly();
            sinkReplicationServer = null;
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

    public void startSourceLogReplicator() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Source Site
                sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, sourceSiteCorfuPort,
                    pluginConfigFilePath, lockLeaseDuration, transportType);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--corfu-server-connection-port=" + sourceSiteCorfuPort,
                            String.valueOf(sourceReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void startSinkLogReplicator() {
        try {
            if (runProcess) {
                // Start Log Replication Server on Source Site
                sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, sinkSiteCorfuPort,
                    pluginConfigFilePath, lockLeaseDuration, transportType);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", "--corfu-server-connection-port=" + sinkSiteCorfuPort,
                            String.valueOf(sinkReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void verifyDataOnSinkNonUFO(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapASink.count() != expectedConsecutiveWrites) {
            // Block until expected number of entries is reached
            log.trace("Map size: {}, expect size: {}",
                    mapASink.count(), expectedConsecutiveWrites);
        }

        log.debug("Number updates on Sink :: " + expectedConsecutiveWrites);

        // Verify data is present in Sink Site
        assertThat(mapASink.count()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            try (TxnContext tx = corfuStoreSink.txn(NAMESPACE)) {
                assertThat(tx.getRecord(mapASink, Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build())
                        .getPayload().getValue()).isEqualTo(i);
                tx.commit();
            }
        }
    }

    public void verifyDataOnSink(int expectedConsecutiveWrites) {
        for(Map.Entry<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> entry : mapNameToMapSink.entrySet()) {

            log.info("Verify Data on Sink's Table {}", entry.getKey());

            // Wait until data is fully replicated
            while (entry.getValue().count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.info("Number updates on Sink Map {} :: {} ", entry.getKey(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(entry.getValue().count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(entry.getValue().getNamespace())) {
                    assertThat(tx.getRecord(entry.getValue(),
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload()).isNotNull();
                    tx.commit();
                }
            }
        }
    }

    public void verifyInLogEntrySyncState() throws InterruptedException {
        LogReplicationSession session = LogReplicationSession.newBuilder()
            .setSourceClusterId(new DefaultClusterConfig().getSourceClusterIds().get(0))
            .setSinkClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
            .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
            .build();

        ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(SyncType.LOG_ENTRY)
                || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                .equals(SyncStatus.COMPLETED)) {
            TimeUnit.SECONDS.sleep(1);
            try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
                status = (ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE_NAME, session).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(status.getSourceStatus().getReplicationInfo().getSyncType()).isEqualTo(SyncType.LOG_ENTRY);
        assertThat(status.getSourceStatus().getReplicationInfo().getStatus())
                .isEqualTo(SyncStatus.ONGOING);

        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getType())
                .isEqualTo(SnapshotSyncType.DEFAULT);
        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                .isEqualTo(SyncStatus.COMPLETED);
    }

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param source true, checkpoint/trim on source cluster
     *               false, checkpoint/trim on sink cluster
     */
    public void checkpointAndTrim(boolean source) {
        CorfuRuntime cpRuntime;

        if (source) {
            cpRuntime = createRuntimeWithCache(sourceEndpoint);
        } else {
            cpRuntime = createRuntimeWithCache(sinkEndpoint);
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
     * Stream Listener used for testing streaming on sink site. This listener decreases a latch
     * until all expected updates are received/
     */
    public static class StreamingSinkListener implements StreamListener {

        private final CountDownLatch updatesLatch;
        private final CountDownLatch numTxLatch;
        public List<CorfuStreamEntry> messages = new ArrayList<>();
        private final Set<UUID> tablesToListenTo;

        public StreamingSinkListener(CountDownLatch updatesLatch, CountDownLatch numTxLatch, Set<UUID> tablesToListenTo) {
            this.updatesLatch = updatesLatch;
            this.tablesToListenTo = tablesToListenTo;
            this.numTxLatch = numTxLatch;
        }

        @Override
        public synchronized void onNext(CorfuStreamEntries results) {
            log.info("StreamingSinkListener:: onNext {} with entry size {}", results, results.getEntries().size());
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
