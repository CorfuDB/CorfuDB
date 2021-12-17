package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;


import com.google.common.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
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
    public Process activeCorfu = null;
    public Process standbyCorfu = null;

    public Process activeReplicationServer = null;
    public Process standbyReplicationServer = null;

    public final String streamA = "Table001";

    public final int numWrites = 5000;
    public final int lockLeaseDuration = 10;

    public final int activeSiteCorfuPort = 9000;
    public final int standbySiteCorfuPort = 9001;

    public final int activeReplicationServerPort = 9010;
    public final int standbyReplicationServerPort = 9020;

    public final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    public final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    public CorfuRuntime activeRuntime;
    public CorfuRuntime standbyRuntime;

    public CorfuTable<String, Integer> mapA;
    public CorfuTable<String, Integer> mapAStandby;

    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapActive;
    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapStandby;

    public CorfuStore corfuStoreActive;
    public CorfuStore corfuStoreStandby;
    private final String author = "checkpointer";

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
            assertThat(mapA.size()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Cluster
            assertThat(mapAStandby.size()).isEqualTo(0);

            startLogReplicatorServers();

            log.debug("Wait ... Snapshot log replication in progress ...");
            verifyDataOnStandbyNonUFO(numWrites);

            // Add Delta's for Log Entry Sync
            writeToActiveNonUFO(numWrites, numWrites/2);

            log.debug("Wait ... Delta log replication in progress ...");
            verifyDataOnStandbyNonUFO((numWrites + (numWrites / 2)));
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

    public void testEndToEndSnapshotAndLogEntrySyncUFO() throws Exception {
        testEndToEndSnapshotAndLogEntrySyncUFO(1);
    }

    public void testEndToEndSnapshotAndLogEntrySyncUFO(int totalNumMaps) throws Exception {
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
                    LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
                    LogReplicationMetadata.ReplicationStatusKey.class,
                    LogReplicationMetadata.ReplicationStatusVal.class,
                    null,
                    TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

            CountDownLatch statusUpdateLatch = new CountDownLatch(totalStandbyStatusUpdates);
            ReplicationStatusListener standbyListener = new ReplicationStatusListener(statusUpdateLatch);
            corfuStoreStandby.subscribeListener(standbyListener, LogReplicationMetadataManager.NAMESPACE,
                    LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

            log.info(">> Open map(s) on active and standby");
            openMaps(totalNumMaps);

            log.info(">> Write data to active CorfuDB before LR is started ...");
            // Add Data for Snapshot Sync
            writeToActive(0, numWrites);

            // Confirm data does exist on Active Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapActive.values()) {
                assertThat(map.count()).isEqualTo(numWrites);
            }

            // Confirm data does not exist on Standby Cluster
            for(Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> map : mapNameToMapStandby.values()) {
                assertThat(map.count()).isEqualTo(0);
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

    private void verifyReplicationStatusFromActive() throws Exception {
        Table<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, LogReplicationMetadata.ReplicationStatusVal>
                replicationStatusTable = corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE,
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

    private void validateSnapshotSyncPlugin(SnapshotSyncPluginListener listener) {
        assertThat(listener.getUpdates().size()).isEqualTo(2);
        assertThat(listener.getUpdates()).contains(DefaultSnapshotSyncPlugin.ON_START_VALUE);
        assertThat(listener.getUpdates()).contains(DefaultSnapshotSyncPlugin.ON_END_VALUE);
    }

    private void subscribeToSnapshotSyncPluginTable(SnapshotSyncPluginListener listener) {
        try {
            corfuStoreStandby.openTable(DefaultSnapshotSyncPlugin.NAMESPACE,
                    DefaultSnapshotSyncPlugin.TABLE_NAME, ExampleSchemas.Uuid.class, SnapshotSyncPluginValue.class,
                    SnapshotSyncPluginValue.class, TableOptions.fromProtoSchema(SnapshotSyncPluginValue.class));
            corfuStoreStandby.subscribeListener(listener, DefaultSnapshotSyncPlugin.NAMESPACE, DefaultSnapshotSyncPlugin.TAG);
        } catch (Exception e) {
            fail("Exception while attempting to subscribe to snapshot sync plugin table");
        }
    }

    private class SnapshotSyncPluginListener implements StreamListener {

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
        private List<Boolean> accumulatedStatus = new ArrayList<>();

        private CountDownLatch countDownLatch;

        public ReplicationStatusListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e ->
                    accumulatedStatus.add(((LogReplicationMetadata.ReplicationStatusVal)e.getPayload()).getDataConsistent())));
            countDownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for ReplicationStatusListener");
        }

        void updateCountdownLatch(CountDownLatch newCountdownLatch) {
            countDownLatch = newCountdownLatch;
        }
    }

    public void setupActiveAndStandbyCorfu() throws Exception {
        try {
            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            // Setup runtime's to active and standby Corfu
            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            activeRuntime = CorfuRuntime.fromParameters(params);
            activeRuntime.parseConfigurationString(activeEndpoint);
            activeRuntime.connect();

            standbyRuntime = CorfuRuntime.fromParameters(params);
            standbyRuntime.parseConfigurationString(standbyEndpoint);
            standbyRuntime.connect();

            corfuStoreActive = new CorfuStore(activeRuntime);
            corfuStoreStandby = new CorfuStore(standbyRuntime);
        } catch (Exception e) {
            log.debug("Error while starting Corfu");
            throw  e;
        }
    }

    public void openMaps(int mapCount) throws Exception {
        mapNameToMapActive = new HashMap<>();
        mapNameToMapStandby = new HashMap<>();

        for(int i=1; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapActive = corfuStoreActive.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class));

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapStandby = corfuStoreStandby.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class));

            mapNameToMapActive.put(mapName, mapActive);
            mapNameToMapStandby.put(mapName, mapStandby);

            assertThat(mapActive.count()).isEqualTo(0);
            assertThat(mapStandby.count()).isEqualTo(0);
        }
    }

    public void openMap() {
        // Write to StreamA on Active Site
        mapA = activeRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        mapAStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(streamA)
                .setStreamTags(ObjectsView.getLogReplicatorStreamId())
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(mapA.size()).isEqualTo(0);
        assertThat(mapAStandby.size()).isEqualTo(0);
    }

    public void writeToActiveNonUFO(int startIndex, int totalEntries) {
        int maxIndex = totalEntries + startIndex;
        for (int i = startIndex; i < maxIndex; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapA.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
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
        while (mapAStandby.size() != expectedConsecutiveWrites) {
            // Block until expected number of entries is reached
        }

        log.debug("Number updates on Standby :: " + expectedConsecutiveWrites);

        // Verify data is present in Standby Site
        assertThat(mapAStandby.size()).isEqualTo(expectedConsecutiveWrites);

        for (int i = 0; i < (expectedConsecutiveWrites); i++) {
            assertThat(mapAStandby.containsKey(String.valueOf(i)));
        }
    }

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param active true, checkpoint/trim on active cluster
     *               false, checkpoint/trim on standby cluster
     * @param tables additional tables (asides CorfuStore to be checkpointed)
     */
    public void checkpointAndTrim(boolean active, List<CorfuTable> tables) {
        CorfuRuntime cpRuntime;

        if (active) {
            cpRuntime = new CorfuRuntime(activeEndpoint).connect();
        } else {
            cpRuntime = new CorfuRuntime(standbyEndpoint).connect();
        }

        // Checkpoint specified tables
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        Token trimMark = null;
        if (tables.size() != 0) {
            mcw.addAllMaps(tables);
            trimMark = mcw.appendCheckpoints(cpRuntime, author);
        }

        checkpointAndTrimCorfuStore(cpRuntime, trimMark);
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

    /**
     * Checkpoint and Trim Data Logs
     *
     * @param active true, checkpoint/trim on active cluster
     *               false, checkpoint/trim on standby cluster
     */
    public void checkpointAndTrim(boolean active) {
        CorfuRuntime cpRuntime;

        if (active) {
            cpRuntime = new CorfuRuntime(activeEndpoint).connect();
        } else {
            cpRuntime = new CorfuRuntime(standbyEndpoint).connect();
        }

        checkpointAndTrimCorfuStore(cpRuntime);
        cpRuntime.shutdown();
    }

    public void checkpointAndTrimCorfuStore(CorfuRuntime cpRuntime) {
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

        Token trimMark = null;

        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                    tableName.getNamespace(), tableName.getTableName()
            );
            SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder = cpRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                    .setStreamName(fullTableName)
                    .setSerializer(dynamicProtoBufSerializer);

            log.info("Checkpointing - {}", fullTableName);
            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());

            Token token = mcw.appendCheckpoints(cpRuntime, author);
            trimMark = trimMark == null ? token : Token.min(trimMark, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(cpRuntime, author);
        trimMark = trimMark != null ? Token.min(trimMark, token) : token;

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
            Token token = mcw.appendCheckpoints(cpRuntime, author);
            trimMark = trimMark == null ? token : Token.min(trimMark, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(cpRuntime, author);
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
