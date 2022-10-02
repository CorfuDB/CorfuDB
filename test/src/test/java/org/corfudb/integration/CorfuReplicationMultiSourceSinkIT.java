package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.After;
import org.junit.Assert;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LR_STATUS_STREAM_TAG;

@Slf4j
public class CorfuReplicationMultiSourceSinkIT extends AbstractIT {
    private final List<Integer> sourceCorfuPorts = Arrays.asList(9000, 9002, 9004);
    private final List<Integer> sinkCorfuPorts = Arrays.asList(9001, 9003, 9005);

    private final List<Integer> sourceReplicationPorts = Arrays.asList(9010, 9011, 9012);
    private final List<Integer> sinkReplicationPorts = Arrays.asList(9020, 9021, 9022);

    private final List<Process> sourceCorfuProcesses = new ArrayList<>();
    private final List<Process> sinkCorfuProcesses = new ArrayList<>();

    private final List<Process> sourceReplicationServers = new ArrayList<>();
    private final List<Process> sinkReplicationServers = new ArrayList<>();

    private List<CorfuRuntime> sourceRuntimes = new ArrayList<>();
    private List<CorfuRuntime> sinkRuntimes = new ArrayList<>();

    protected List<CorfuStore> sourceCorfuStores = new ArrayList<>();
    protected List<CorfuStore> sinkCorfuStores = new ArrayList<>();

    private final List<String> sourceEndpoints = new ArrayList<>();
    private final List<String> sinkEndpoints = new ArrayList<>();

    private int numSourceClusters;
    private int numSinkClusters;

    private static final String TABLE_1 = "Table001";
    private static final String TABLE_2 = "Table002";
    private static final String TABLE_3 = "Table003";
    protected final List<String> tableNames = Arrays.asList(TABLE_1, TABLE_2, TABLE_3);

    protected final List<Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message>> srcTables = new ArrayList<>();
    protected final List<Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message>> sinkTables = new ArrayList<>();

    protected static final String NAMESPACE = DefaultLogReplicationConfigAdapter.NAMESPACE;

    protected static final String STREAM_TAG = DefaultLogReplicationConfigAdapter.TAG_ONE;

    // DefaultClusterConfig contains 3 Source and Sink clusters each.  Depending on how many clusters the test
    // starts, the number of functional/available clusters may be less but 3 is the max number.
    protected static final int MAX_REMOTE_CLUSTERS = 3;

    // The number of updates on the ReplicationStatus table on the Sink during initial startup is 3(one for each
    // Source cluster - the number of available Source clusters may be <3 but the topology from DefaultClusterConfig
    // contains 3 Source clusters)
    protected static final int NUM_INITIAL_REPLICATION_STATUS_UPDATES = MAX_REMOTE_CLUSTERS;

    // The number of updates on the Sink ReplicationStatus Table during Snapshot Sync from single cluster
    // (1) When starting snapshot sync apply : is_data_consistent = false
    // (2) When completing snapshot sync apply : is_data_consistent = true
    protected static final int NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE = 2;

    protected static final int NUM_RECORDS_IN_TABLE = 3;

    protected String pluginConfigFilePath;

    // Listens to incoming data on streams/tables on a Sink cluster
    private List<ReplicatedStreamsListener> dataListeners = new ArrayList<>();

    // Listens to replication status updates on a Sink cluster
    private List<ReplicationStatusListener> replicationStatusListeners = new ArrayList<>();

    /*protected ReplicationStatusListener sourceListener1;
    protected ReplicationStatusListener sourceListener2;
    protected ReplicationStatusListener sourceListener3;
    protected ReplicationStatusListener sinkListener1;
    protected ReplicationStatusListener sinkListener2;
    protected ReplicationStatusListener sinkListener3;*/

    protected void setUp(int numSourceClusters, int numSinkClusters) throws Exception {
        this.numSourceClusters = numSourceClusters;
        this.numSinkClusters = numSinkClusters;
        setupSourceAndSinkCorfu(numSourceClusters, numSinkClusters);
    }

    private void setupSourceAndSinkCorfu(int numSourceClusters, int numSinkClusters) throws Exception {
        Process process;
        String endpoint;
        for (int i = 0; i < numSourceClusters; i++) {
            process = runServer(sourceCorfuPorts.get(i), true);
            sourceCorfuProcesses.add(process);
            endpoint = DEFAULT_HOST + ":" + sourceCorfuPorts.get(i).toString();
            sourceEndpoints.add(endpoint);
        }

        for (int i = 0; i < numSinkClusters; i++) {
            process = runServer(sinkCorfuPorts.get(i), true);
            sinkCorfuProcesses.add(process);
            endpoint = DEFAULT_HOST + ":" + sinkCorfuPorts.get(i).toString();
            sinkEndpoints.add(endpoint);
        }

        // Setup the runtimes to each Corfu server
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
            .builder()
            .build();

        CorfuRuntime runtime;
        for (int i = 0; i<numSourceClusters; i++) {
            runtime = CorfuRuntime.fromParameters(params);
            runtime.parseConfigurationString(sourceEndpoints.get(i));
            runtime.connect();
            sourceRuntimes.add(runtime);

            sourceCorfuStores.add(new CorfuStore(runtime));
        }

        for (int i = 0; i<numSinkClusters; i++) {
            runtime = CorfuRuntime.fromParameters(params);
            runtime.parseConfigurationString(sinkEndpoints.get(i));
            runtime.connect();
            sinkRuntimes.add(runtime);

            sinkCorfuStores.add(new CorfuStore(runtime));
        }
    }

    protected void startReplicationServers() throws Exception {
        for (int i = 0; i < numSourceClusters; i++) {
            sourceReplicationServers.add(runReplicationServer(sourceReplicationPorts.get(i), pluginConfigFilePath));
        }

        for (int i = 0; i < numSinkClusters; i++) {
            sinkReplicationServers.add(runReplicationServer(sinkReplicationPorts.get(i), pluginConfigFilePath));
        }
    }

    protected void openMaps() throws Exception {
        Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table;

        // Maps are opened as per number of Source sites.  The assumption is that Table001 is opened and written to
        // on Source 1, Table002 on Source 2 and so on.
        for (int i = 0; i < numSourceClusters; i++) {
            table = sourceCorfuStores.get(i).openTable(NAMESPACE, tableNames.get(i), Sample.StringKey.class,
                SampleSchema.ValueFieldTagOne.class, null,
                TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
            srcTables.add(table);

            // Open the same tables on the Sink clusters to get stream listener updates.
            for (int j = 0; j < numSinkClusters; j++) {
                table = sinkCorfuStores.get(j).openTable(NAMESPACE, tableNames.get(i), Sample.StringKey.class,
                    SampleSchema.ValueFieldTagOne.class, null,
                    TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
                sinkTables.add(table);
            }
        }
    }

    protected void writeData(CorfuStore corfuStore, String tableName, Table table, int startIndex, int numRecords) {
        for (int i = startIndex; i < (startIndex + numRecords); i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(tableName + " key " + i).build();
            SampleSchema.ValueFieldTagOne payload = SampleSchema.ValueFieldTagOne.newBuilder().setPayload(
                tableName + " payload " + i).build();
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, key, payload, null);
                txn.commit();
            }
        }
    }

    protected void deleteRecord(CorfuStore corfuStore, String tableName, int index) {
        Sample.StringKey key = Sample.StringKey.newBuilder().setKey(tableName + " key " + index).build();

        try (TxnContext txnContext = corfuStore.txn(NAMESPACE)) {
            txnContext.delete(tableName, key);
            txnContext.commit();
        }
    }

    protected int calculateSnapshotSyncUpdatesOnSinkStatusTable(int numSourceClusters) {
        return numSourceClusters * NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE;
    }

    protected void verifySnapshotAndLogEntrySink(boolean changeRole) throws Exception {
        // Open maps on the Source and Destination Sites.  Maps are opened as per number of Source sites.  The
        // assumption is that Table001 is opened and written to on Source 1, Table002 on Source 2 and so on.
        openMaps();

        log.debug("Write 3 records locally on each Source Cluster");

        // Write data to tables opened on each source cluster
        for (int i = 0; i < numSourceClusters; i++) {
            writeData(sourceCorfuStores.get(i), tableNames.get(i), srcTables.get(i), 0, NUM_RECORDS_IN_TABLE);
        }

        // Register a stream listener on the Sink clusters to listen for incoming replication data writes.
        // During snapshot sync, there will be 1 write(with all entries) for each replicated table with data.  The
        // listener ignores 'single clear' writes('clear' on a table from Snapshot sync from another Source which
        // does not have data on this table) for the countdown latch.  So there will be 1 write per Source cluster,
        // i.e., Source Cluster 1's writes to Table001 and so on.
        // Also subscribe to updates on the Replication Status table.
        List<CountDownLatch> snapshotWritesLatches = new ArrayList<>();
        CountDownLatch dataLatch;
        ReplicatedStreamsListener dataListener;

        // On startup, an initial default replication status is written for each remote cluster
        // (NUM_INITIAL_REPLICATION_STATUS_UPDATES).  Subsequently, the table will be updated on snapshot sync from
        // each Source cluster.
        int numExpectedUpdates = NUM_INITIAL_REPLICATION_STATUS_UPDATES +
            calculateSnapshotSyncUpdatesOnSinkStatusTable(numSourceClusters);
        List<CountDownLatch> statusLatches = new ArrayList<>();
        CountDownLatch statusLatch;
        ReplicationStatusListener statusListener;

        for (int i = 0; i < numSinkClusters; i++) {
            // Data Listeners.
            // In the test, the number of tables where updates are applied = numSourceClusters(Source 1 - Table001,
            // Source2 - Table002..).  So there will be numSourceClusters number of transactions.
            dataLatch = new CountDownLatch(numSourceClusters);
            snapshotWritesLatches.add(dataLatch);

            dataListener = new ReplicatedStreamsListener(dataLatch, true);
            dataListeners.add(dataListener);

            sinkCorfuStores.get(i).subscribeListener(dataListener, NAMESPACE, STREAM_TAG);

            // Replication Status Listeners
            sinkCorfuStores.get(i).openTable(LogReplicationMetadataManager.NAMESPACE,
                LogReplicationMetadataManager.REPLICATION_STATUS_TABLE, LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class, null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
            statusLatch = new CountDownLatch(numExpectedUpdates);
            statusLatches.add(statusLatch);

            statusListener = new ReplicationStatusListener(statusLatch);
            replicationStatusListeners.add(statusListener);
            sinkCorfuStores.get(i).subscribeListener(statusListener, LogReplicationMetadataManager.NAMESPACE,
                LR_STATUS_STREAM_TAG);
        }

        if (changeRole) {
            Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg>
                configTable = sinkCorfuStores.get(0).openTable(DefaultClusterManager.CONFIG_NAMESPACE,
                    DefaultClusterManager.CONFIG_TABLE_NAME, ExampleSchemas.ClusterUuidMsg.class,
                    ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                    TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class));
            try (TxnContext txn = sinkCorfuStores.get(0).txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
                txn.putRecord(configTable, DefaultClusterManager.OP_SWITCH, DefaultClusterManager.OP_SWITCH,
                    DefaultClusterManager.OP_SWITCH);
                txn.commit();
            }
            Assert.assertEquals(1, configTable.count());

        } else {
            // Start Log Replication
            startReplicationServers();
        }

        // Verify the number of updates and order of operations received on snapshot sync
        // During snapshot sync, a 'clear' followed by 'update' for each record is received.  So there should be
        // 1 Clear + NUM_RECORDS_IN_TABLE Update records for each table on each Sink cluster.
        List<CorfuStreamEntry.OperationType> expectedOpsList = Arrays.asList(CorfuStreamEntry.OperationType.CLEAR,
            CorfuStreamEntry.OperationType.UPDATE, CorfuStreamEntry.OperationType.UPDATE,
            CorfuStreamEntry.OperationType.UPDATE);
        for (int i = 0; i < numSinkClusters; i++) {
            statusLatches.get(i).await();
            snapshotWritesLatches.get(i).await();

            for (int j = 0; j < srcTables.size(); j++) {
                Assert.assertEquals(NUM_RECORDS_IN_TABLE + 1,
                    dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(j)).size());
                Assert.assertTrue(Objects.equals(expectedOpsList,
                    dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(j))));
            }
        }

        // Verify LogEntry Sync by writing and deleting a record each.
        // If there are multiple Source clusters, UPDATE and DELETE are done on different tables, each on a separate
        // Source Cluster.  Number of expected transactions on the Sink = 2 (1 from each Source and table)

        // In case of a topology with a single Source cluster, perform both ops on the same table(because number of
        // tables opened = number of Source clusters).  During Log Entry Sync, there is transaction batching on the
        // Source cluster, so both operations will be received and applied in the same transaction on the Sink.
        // Hence, number of expected transactions = 1;
        List<CountDownLatch> logEntryWritesLatches = new ArrayList<>();
        CountDownLatch logEntryWritesLatch;
        for (int i = 0; i < numSinkClusters; i++) {
            if (numSourceClusters > 1) {
                logEntryWritesLatch = new CountDownLatch(2);
            } else {
                logEntryWritesLatch = new CountDownLatch(1);
            }
            logEntryWritesLatches.add(logEntryWritesLatch);
            dataListeners.get(i).setCountdownLatch(logEntryWritesLatch);
            dataListeners.get(i).clearTableToOpTypeMap();
            dataListeners.get(i).setSnapshotSync(false);
        }
        log.info("Add a record on table on Sender-1, Table-1");
        writeData(sourceCorfuStores.get(0), tableNames.get(0), srcTables.get(0), NUM_RECORDS_IN_TABLE, 1);

        log.info("Delete Key2");
        if (numSourceClusters > 1) {
            log.info("Delete a record from Sender-2, Table-2");
            deleteRecord(sourceCorfuStores.get(1), tableNames.get(1), 2);
        } else {
            log.info("Delete a record from Sender-1, Table-1");
            deleteRecord(sourceCorfuStores.get(0), tableNames.get(0), 2);
        }

        for (int i = 0; i < numSinkClusters; i++) {
            logEntryWritesLatches.get(i).await();

            if (numSourceClusters > 1) {
                // 1 Operation received for each table on the Sink cluster
                Assert.assertEquals(1, dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(0)).size());
                Assert.assertEquals(CorfuStreamEntry.OperationType.UPDATE,
                    dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(0)).get(0));
                Assert.assertEquals(1, dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(1)).size());
                Assert.assertEquals(CorfuStreamEntry.OperationType.DELETE,
                    dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(1)).get(0));
            } else {
                // 2 operations received for the same table on the Sink cluster
                Assert.assertEquals(2, dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(0)).size());

                // As these operations are applied in the same transaction, their streaming updates can come in any
                // order(to be fixed).  We can only verify that both UPDATE and DELETE are received.
                Assert.assertTrue(dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(0)).contains(
                    CorfuStreamEntry.OperationType.UPDATE));
                Assert.assertTrue(dataListeners.get(i).getTableToOpTypeMap().get(tableNames.get(0)).contains(
                    CorfuStreamEntry.OperationType.DELETE));
            }
        }
    }

    /**
     * On a role change, the number of Source and Sink clusters changes.  The roles of clusters in the topology
     * change, but the total number of clusters is the same.  Additionally, the expected behavior on snapshot and log
     * entry sync is the same, only the senders and receivers are different.  This method resets and exchanges the test
     * state, making it ready for role change, such that the same validation method(verifySnapshotAndLogEntrySync) can
     * be reused after the change.
     * @param numNewSourceClusters
     * @param numNewSinkClusters
     */
    protected void prepareTestTopologyForRoleChange(int numNewSourceClusters, int numNewSinkClusters) {
        // The test workflow is as follows:
        // 1. verifySnapshotAndLogEntrySync
        // 2. change the role
        // 3. verifySnapshotAndLogEntrySync ( with role changed)
        // Clear data from all Source clusters so that verifySnapshotAndLogEntrySync can re-use existing tables to
        // write and perform the same validation.
        for (int i = 0; i < numSourceClusters; i++) {
            try (TxnContext txn = sourceCorfuStores.get(i).txn(NAMESPACE)) {
                txn.clear(tableNames.get(i));
                txn.commit();
            }
        }

        for (Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table : srcTables) {
            Assert.assertEquals(0, table.count());
        }

        // Verify that no tables on the Sink cluster have data
        for (Table<Sample.StringKey, SampleSchema.ValueFieldTagOne, Message> table : sinkTables) {
            while(table.count() != 0) {

            }
        }
        // Reset/clear the list of tables on Source and Sink
        srcTables.clear();
        sinkTables.clear();

        // Unsubscribe the listeners on Sink clusters
        unsubscribeListeners();

        // Reset/clear the list of listeners
        dataListeners.clear();
        replicationStatusListeners.clear();

        // The test workflow is as follows:
        // 1. verifySnapshotAndLogEntrySync
        // 2. change the role
        // 3. verifySnapshotAndLogEntrySync ( with role changed)
        // verifySnapshotAndLogEntrySync checks the number of data updates received on a snapshot and log entry sync.
        // It assumes that the Sink table/s are empty when snapshot sync started.
        // However, in step 3, the streams will have older updates from:
        // - Step 1 and
        // - Clear operation done at the beginning of this method
        // So run a CP+Trim on the to-be Source(current Sink) clusters so that the above redundant updates get
        // Checkpointed and are not sent.
        for (int i = 0; i < numSinkClusters; i++) {
            CorfuRuntime cpRuntime = new CorfuRuntime(sinkEndpoints.get(i)).connect();
            checkpointAndTrimCorfuStore(cpRuntime);
        }

        // Exchange source and sink corfu stores
        List<CorfuStore> tmp = sourceCorfuStores;
        sourceCorfuStores = sinkCorfuStores;
        sinkCorfuStores = tmp;

        numSourceClusters = numNewSourceClusters;
        numSinkClusters = numNewSinkClusters;
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

    private void unsubscribeListeners() {
        for (int i = 0; i < numSinkClusters; i++) {
            sinkCorfuStores.get(i).unsubscribeListener(dataListeners.get(i));
            sinkCorfuStores.get(i).unsubscribeListener(replicationStatusListeners.get(i));
        }
    }

    @After
    public void tearDown() {
        unsubscribeListeners();
        shutdownCorfuServers();
        shutdownLogReplicationServers();

        for (CorfuRuntime runtime : sourceRuntimes) {
            runtime.shutdown();
        }
        for (CorfuRuntime runtime : sinkRuntimes) {
            runtime.shutdown();
        }
    }

    private void shutdownCorfuServers() {
        for (Process process : sourceCorfuProcesses) {
            process.destroy();
        }

        for (Process process : sinkCorfuProcesses) {
            process.destroy();
        }
    }

    private void shutdownLogReplicationServers() {
        for (Process lrProcess : sourceReplicationServers) {
            lrProcess.destroy();
        }

        for (Process lrProcess : sinkReplicationServers) {
            lrProcess.destroy();
        }
    }

    protected class ReplicationStatusListener implements StreamListener {

        @Getter
        List<Boolean> accumulatedStatus = new ArrayList<>();

        private final CountDownLatch countDownLatch;

        public ReplicationStatusListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            // Replication Status Table gets cleared on a role change.  Ignore the 'clear' updates
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                if (e.getOperation() != CorfuStreamEntry.OperationType.CLEAR) {
                    accumulatedStatus.add(((LogReplicationMetadata.ReplicationStatusVal)e.getPayload()).getDataConsistent());
                    countDownLatch.countDown();
                }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error: ", throwable);
            fail("onError for ReplicationStatusListener");
        }
    }

    protected class ReplicatedStreamsListener implements StreamListener {

        @Getter
        Map<String, List<CorfuStreamEntry.OperationType>> tableToOpTypeMap = new HashMap<>();

        @Setter
        private CountDownLatch countdownLatch;

        @Setter
        private boolean snapshotSync;

        public ReplicatedStreamsListener(CountDownLatch countdownLatch, boolean snapshotSync) {
            this.countdownLatch = countdownLatch;
            this.snapshotSync = snapshotSync;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> {
                if (snapshotSync) {
                    if (entries.size() > 1) {
                        processUpdate(schema, entries);
                    }
                } else {
                    processUpdate(schema, entries);
                }
            });
        }

        private void processUpdate(TableSchema schema, List<CorfuStreamEntry> entries) {
            List<CorfuStreamEntry.OperationType> opList = tableToOpTypeMap.getOrDefault(
                schema.getTableName(), new ArrayList<>());
            entries.forEach(entry -> opList.add(entry.getOperation()));
            tableToOpTypeMap.put(schema.getTableName(), opList);
            countdownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error: ", throwable);
            fail("onError for ReplicatedStreamsListener");
        }

        public void clearTableToOpTypeMap() {
            tableToOpTypeMap.clear();
        }
    }
}
