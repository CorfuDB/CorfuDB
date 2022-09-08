package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;


import com.google.common.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.Sleep;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

@Slf4j
public class LogReplicationAbstractIT extends AbstractIT {

    private static final int MSG_SIZE = 65536;

    private static final int SHORT_SLEEP_TIME = 10;

    public static final String TABLE_PREFIX = "Table00";

    public static final String NAMESPACE = "LR-Test";

    public final static String nettyConfig = "src/test/resources/transport/nettyConfig.properties";

    public String pluginConfigFilePath;

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

    public final int numWrites = 5000;
    public final int lockLeaseDuration = 10;

    public final int sourceSiteCorfuPort = 9000;
    public final int sinkSiteCorfuPort = 9001;

    public final int sourceReplicationServerPort = 9010;
    public final int sinkReplicationServerPort = 9020;

    public final String sourceEndpoint = DEFAULT_HOST + ":" + sourceSiteCorfuPort;
    public final String sinkEndpoint = DEFAULT_HOST + ":" + sinkSiteCorfuPort;

    public CorfuRuntime sourceRuntime;
    public CorfuRuntime sinkRuntime;

    public CorfuTable<String, Integer> mapA;
    public CorfuTable<String, Integer> mapASink;

    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapSource;
    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapSink;

    public CorfuStore corfuStoreSource;
    public CorfuStore corfuStoreSink;

    private final long longInterval = 20L;

    public void testEndToEndSnapshotAndLogEntrySync() throws Exception {
        try {
            log.debug("Setup source and sink Corfu's");
            setupSourceAndSinkCorfu();

            log.debug("Open map on source and sink");
            openMap();

            log.debug("Write data to source CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToSourceNonUFO(0, numWrites);

            // Confirm data does exist on Source Cluster
            assertThat(mapA.size()).isEqualTo(numWrites);

            // Confirm data does not exist on Sink Cluster
            assertThat(mapASink.size()).isEqualTo(0);

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnSinkNonUFO(numWrites);

            // Add Delta's for Log Entry Sync
            writeToSourceNonUFO(numWrites, numWrites/2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyDataOnSinkNonUFO((numWrites + (numWrites / 2)));
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

    public void testEndToEndSnapshotAndLogEntrySyncUFO(boolean diskBased, boolean checkRemainingEntriesOnSecondLogEntrySync) throws Exception {
        testEndToEndSnapshotAndLogEntrySyncUFO(1, diskBased, checkRemainingEntriesOnSecondLogEntrySync);
    }

    public void testEndToEndSnapshotAndLogEntrySyncUFO(int totalNumMaps, boolean diskBased, boolean checkRemainingEntriesOnSecondLogEntrySync) throws Exception {
        // For the purpose of this test, Sink should only update status 5 times:
        // (1) On startup, init the replication status for each Source cluster(3 clusters = 3 updates)
        // (2) When starting snapshot sync apply : is_data_consistent = false
        // (3) When completing snapshot sync apply : is_data_consistent = true
        final int totalSinkStatusUpdates = 5;

        try {
            log.info(">> Setup source and sink Corfu's");
            setupSourceAndSinkCorfu();

            // Two updates are expected onStart of snapshot sync and onEnd.
            CountDownLatch latchSnapshotSyncPlugin = new CountDownLatch(2);
            SnapshotSyncPluginListener snapshotSyncPluginListener = new SnapshotSyncPluginListener(latchSnapshotSyncPlugin);
            subscribeToSnapshotSyncPluginTable(snapshotSyncPluginListener);

            // Subscribe to replication status table on Sink (to be sure data change on status are captured)
            corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

            CountDownLatch statusUpdateLatch = new CountDownLatch(totalSinkStatusUpdates);
            ReplicationStatusListener sinkListener = new ReplicationStatusListener(statusUpdateLatch, false);
            corfuStoreSink.subscribeListener(sinkListener, LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

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
                assertThat(map.count()).isEqualTo(0);
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
            verifyDataOnSink((numWrites + (numWrites / 2)));

            // Verify Sink Status Listener received all expected updates (is_data_consistent)
            log.info(">> Wait ... Replication status UPDATE ...");
            statusUpdateLatch.await();
            assertThat(sinkListener.getAccumulatedStatus().size()).isEqualTo(totalSinkStatusUpdates);
            // Confirm last updates are set to true (corresponding to snapshot sync completed and log entry sync started)
            assertThat(sinkListener.getAccumulatedStatus().get(sinkListener.getAccumulatedStatus().size() - 1)).isTrue();
            assertThat(sinkListener.getAccumulatedStatus()).contains(false);

            if (checkRemainingEntriesOnSecondLogEntrySync) {
                triggerSnapshotAndTestRemainingEntries();
            }

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

    private void triggerSnapshotAndTestRemainingEntries() throws Exception{

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

        CountDownLatch statusUpdateLatch = new CountDownLatch(1);
        ReplicationStatusListener sourceListener = new ReplicationStatusListener(statusUpdateLatch, true);
        corfuStoreSource.subscribeListener(sourceListener, LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getSinkClusterIds().get(0))
                        .build();
        LogReplicationMetadata.ReplicationStatusVal replicationStatusVal;

        statusUpdateLatch.await();
        try (TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
            replicationStatusVal = (LogReplicationMetadata.ReplicationStatusVal) txn.getRecord("LogReplicationStatus", key).getPayload();
            txn.commit();
        }

        assertThat(replicationStatusVal.getRemainingEntriesToSend()).isEqualTo(0);
    }

    private void verifyReplicationStatusFromSource() throws Exception {
        Table<LogReplicationMetadata.ReplicationStatusKey,
            LogReplicationMetadata.ReplicationStatusVal,
            LogReplicationMetadata.ReplicationStatusVal> replicationStatusTable =
            corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        String clusterId =
            new DefaultClusterConfig().getSinkClusterIds().get(0);

        LogReplicationMetadata.ReplicationStatusKey key =
            LogReplicationMetadata.ReplicationStatusKey
                .newBuilder()
                .setClusterId(clusterId)
                .build();

        IRetry.build(IntervalRetry.class, () -> {
            try(TxnContext txn = corfuStoreSource.txn(LogReplicationMetadataManager.NAMESPACE)) {
                CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey,
                    LogReplicationMetadata.ReplicationStatusVal,
                    LogReplicationMetadata.ReplicationStatusVal> entry =
                    txn.getRecord(replicationStatusTable, key);

                if (entry.getPayload().getSnapshotSyncInfo().getStatus() !=
                    LogReplicationMetadata.SyncStatus.COMPLETED) {
                    Sleep.sleepUninterruptibly(Duration.ofMillis(SHORT_SLEEP_TIME));
                    txn.commit();
                    throw new RetryNeededException();
                }
                assertThat(entry.getPayload().getSnapshotSyncInfo().getStatus())
                    .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
                assertThat(entry.getPayload().getSnapshotSyncInfo().getType())
                    .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
                assertThat(entry.getPayload().getSnapshotSyncInfo()
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

    class SnapshotSyncPluginListener implements StreamListener {

        @Getter
        List<String> updates = new ArrayList<>();

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

        private final CountDownLatch countDownLatch;
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
            countDownLatch.countDown();
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
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            sourceRuntime = CorfuRuntime.fromParameters(params);
            sourceRuntime.parseConfigurationString(sourceEndpoint);
            sourceRuntime.connect();

            sinkRuntime = CorfuRuntime.fromParameters(params);
            sinkRuntime.parseConfigurationString(sinkEndpoint);
            sinkRuntime.connect();

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

            assertThat(mapSource.count()).isEqualTo(0);
            assertThat(mapSink.count()).isEqualTo(0);
        }
    }

    public void openMap() {
        // Write to StreamA on Source Site
        mapA = sourceRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        mapASink = sinkRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(mapA.size()).isEqualTo(0);
        assertThat(mapASink.size()).isEqualTo(0);
    }

    public void writeToSourceNonUFO(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            sourceRuntime.getObjectsView().TXBegin();
            mapA.put(String.valueOf(i), i);
            sourceRuntime.getObjectsView().TXEnd();
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
                    runReplicationServer(sourceReplicationServerPort, pluginConfigFilePath,
                        lockLeaseDuration);

                // Start Log Replication Server on Sink Site
                sinkReplicationServer =
                    runReplicationServer(sinkReplicationServerPort, pluginConfigFilePath,
                        lockLeaseDuration);
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
                sourceReplicationServer = runReplicationServer(sourceReplicationServerPort, pluginConfigFilePath, lockLeaseDuration);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", String.valueOf(sourceReplicationServerPort)});
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
                sinkReplicationServer = runReplicationServer(sinkReplicationServerPort, pluginConfigFilePath, lockLeaseDuration);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"-m", "--plugin=" + pluginConfigFilePath,
                            "--address=localhost", String.valueOf(sinkReplicationServerPort)});
                });
            }
        } catch (Exception e) {
            log.debug("Error caught while running Log Replication Server");
        }
    }

    public void verifyDataOnSinkNonUFO(int expectedConsecutiveWrites) {
        // Wait until data is fully replicated
        while (mapASink.size() != expectedConsecutiveWrites) {
            // Block until expected number of entries is reached
        }

        log.debug("Number updates on Sink :: " + expectedConsecutiveWrites);

        // Verify data is present in Sink Site
        assertThat(mapASink.size()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            assertThat(mapASink.containsKey(String.valueOf(i)));
        }
    }

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param source true, checkpoint/trim on source cluster
     *               false, checkpoint/trim on sink cluster
     * @param tables additional tables (asides CorfuStore to be checkpointed)
     */
    public void checkpointAndTrim(boolean source, List<CorfuTable> tables) {
        CorfuRuntime cpRuntime;

        if (source) {
            cpRuntime = new CorfuRuntime(sourceEndpoint).connect();
        } else {
            cpRuntime = new CorfuRuntime(sinkEndpoint).connect();
        }

        // Checkpoint specified tables
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        Token trimMark = null;
        if (tables.size() != 0) {
            mcw.addAllMaps(tables);
            trimMark = mcw.appendCheckpoints(cpRuntime, "author");
        }

        checkpointAndTrimCorfuStore(cpRuntime, trimMark);
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

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param source true, checkpoint/trim on source cluster
     *               false, checkpoint/trim on sink cluster
     */
    public void checkpointAndTrim(boolean source) {
        CorfuRuntime cpRuntime;

        if (source) {
            cpRuntime = new CorfuRuntime(sourceEndpoint).connect();
        } else {
            cpRuntime = new CorfuRuntime(sinkEndpoint).connect();
        }

        checkpointAndTrimCorfuStore(cpRuntime);
    }

    public void checkpointAndTrimCorfuStore(CorfuRuntime cpRuntime) {
        // Open Table Registry
        TableRegistry tableRegistry = cpRuntime.getTableRegistry();
        CorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                CorfuStoreMetadata.TableMetadata>> tableRegistryCT = tableRegistry.getRegistryTable();

        CorfuTable<CorfuStoreMetadata.ProtobufFileName,
            CorfuRecord<CorfuStoreMetadata.ProtobufFileDescriptor, CorfuStoreMetadata.TableMetadata>>
            protobufDescriptorTable =
            tableRegistry.getProtobufDescriptorTable();

        // Save the regular serializer first..
        ISerializer protoBufSerializer = cpRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtoBufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtoBufSerializer
        ISerializer dynamicProtoBufSerializer = new DynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(dynamicProtoBufSerializer);

        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();

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
            SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder = cpRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                    .setStreamName(fullTableName)
                    .setSerializer(dynamicProtoBufSerializer);

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());

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
    }

    public void checkpointAndTrimCorfuStore(CorfuRuntime cpRuntime, Token trimMark) {
        // Open Table Registry
        TableRegistry tableRegistry = cpRuntime.getTableRegistry();
        CorfuTable<CorfuStoreMetadata.TableName, CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                CorfuStoreMetadata.TableMetadata>> tableRegistryCT = tableRegistry.getRegistryTable();

        // Save the regular serializer first..
        ISerializer protoBufSerializer = cpRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtoBufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtoBufSerializer
        ISerializer dynamicProtoBufSerializer = new DynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(dynamicProtoBufSerializer);

        // First checkpoint the TableRegistry system table
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();

        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                    tableName.getNamespace(), tableName.getTableName()
            );
            SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder = cpRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                    .setStreamName(fullTableName)
                    .setSerializer(dynamicProtoBufSerializer);

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());
            Token token = mcw.appendCheckpoints(cpRuntime, "checkpointer");
            trimMark = trimMark == null ? token : Token.min(trimMark, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(cpRuntime, "checkpointer");
        trimMark = Token.min(trimMark, token);

        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().gc();

        // Lastly restore the regular protoBuf serializer and undo the dynamic protoBuf serializer
        // otherwise the test cannot continue beyond this point.
        cpRuntime.getSerializers().registerSerializer(protoBufSerializer);

        // Trim
        log.debug("**** Trim Log @address=" + trimMark);
        cpRuntime.getAddressSpaceView().prefixTrim(trimMark);
        cpRuntime.getAddressSpaceView().invalidateClientCache();
        cpRuntime.getAddressSpaceView().invalidateServerCaches();
        cpRuntime.getAddressSpaceView().gc();
    }
}
