package org.corfudb.integration;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.DynamicMessage;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Options;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Simple test that inserts data into CorfuStore via a separate server process
 */
public class CorfuStoreIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;
    private CorfuStore corfuStore;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * Basic test that inserts a single item using protobuf defined in the proto/ directory.
     */
    @Test
    public void testTx() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        Table<Uuid, Uuid, Uuid> table = store.openTable(
                "namespace", "table", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;
        Uuid uuidKey = Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        Uuid uuidVal = Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        Uuid metadata = Uuid.newBuilder()
                .setMsb(metadataUuid)
                .setLsb(metadataUuid)
                .build();
        TxnContext tx = store.txn("namespace");
        tx.putRecord(table, uuidKey, uuidVal, metadata);
        tx.commit();
        CorfuRecord record = table.get(uuidKey);
        assertThat(record.getPayload()).isEqualTo(uuidVal);
        assertThat(record.getMetadata()).isEqualTo(metadata);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test is divided into 3 phases.
     * Phase 1: Writes data to CorfuStore in a Table using the transaction builder.
     * Phase 2: Using DynamicMessages, we try to read and edit the message. The lsb in the metadata is putd.
     * Phase 3: Using the corfuStore the message is read back to ensure, the schema wasn't altered and the
     * serialization isn't broken. The 'lsb' value of the metadata is asserted.
     */
    @Test
    public void readDataWithDynamicMessages() throws Exception {

        final String namespace = "namespace";
        final String tableName = "table";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1
        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<Uuid, Uuid, ManagedResources> table = store.openTable(
                namespace,
                tableName,
                Uuid.class,
                Uuid.class,
                ManagedResources.class,
                TableOptions.builder().build());

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long newMetadataUuid = 99L;

        Uuid uuidKey = Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        Uuid uuidVal = Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        ManagedResources metadata = ManagedResources.newBuilder()
                .setCreateTimestamp(keyUuid)
                .build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table, uuidKey, uuidVal, metadata);
        tx.commit();

        runtime.shutdown();

        // PHASE 2
        // Interpret using dynamic messages.
        runtime = createRuntime(singleNodeEndpoint);

        ISerializer dynamicProtobufSerializer = new DynamicProtobufSerializer(runtime);
        Serializers.registerSerializer(dynamicProtobufSerializer);

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
                })
                .setStreamName(TableRegistry.getFullyQualifiedTableName(namespace, tableName))
                .setSerializer(dynamicProtobufSerializer)
                .open();

        for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry : corfuTable.entrySet()) {
            CorfuDynamicKey key = entry.getKey();
            CorfuDynamicRecord value = entry.getValue();
            DynamicMessage metaMsg = value.getMetadata();

            DynamicMessage.Builder newMetaBuilder = metaMsg.toBuilder();
            metaMsg.getAllFields().forEach((fieldDescriptor, o) -> {
                if (fieldDescriptor.getName().equals("create_timestamp")) {
                    newMetaBuilder.setField(fieldDescriptor, newMetadataUuid);
                }
            });

            corfuTable.put(key, new CorfuDynamicRecord(
                    value.getPayloadTypeUrl(),
                    value.getPayload(),
                    value.getMetadataTypeUrl(),
                    newMetaBuilder.build()));
        }

        assertThat(corfuTable.size()).isEqualTo(1);
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> tableRegistry = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
                })
                .setStreamName(TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME))
                .setSerializer(dynamicProtobufSerializer)
                .addOpenOption(ObjectOpenOption.NO_CACHE)
                .open();

        mcw.addMap(corfuTable);
        mcw.addMap(tableRegistry);
        Token trimPoint = mcw.appendCheckpoints(runtime, "checkpointer");

        runtime.getAddressSpaceView().prefixTrim(trimPoint);
        runtime.getAddressSpaceView().gc();
        Serializers.clearCustomSerializers();
        runtime.shutdown();

        // PHASE 3
        // Read using protobuf serializer.
        runtime = createRuntime(singleNodeEndpoint);
        final CorfuStore store3 = new CorfuStore(runtime);

        // Attempting to open an unopened table with the short form should throw the IllegalArgumentException
        assertThatThrownBy(() -> store3.getTable(namespace, tableName)).
        isExactlyInstanceOf(IllegalArgumentException.class);

        // Attempting to open a non-existent table should throw NoSuchElementException
        assertThatThrownBy(() -> store3.getTable(namespace, "NonExistingTableName")).
                isExactlyInstanceOf(NoSuchElementException.class);

        Table<Uuid, Uuid, ManagedResources> table1 = store3.openTable(namespace,
                tableName, Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().build());
        try (TxnContext txn = store3.txn(namespace)) {
            CorfuStoreEntry<Uuid, Uuid, ManagedResources> record = txn.getRecord(tableName, uuidKey);
            assertThat(record.getMetadata().getCreateTimestamp()).isEqualTo(newMetadataUuid);
            txn.putRecord(table1, uuidKey, uuidVal, metadata);
            txn.commit();
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    public Token checkpointAndTrimCorfuStore(CorfuRuntime runtimeC, boolean skipTrim, String tempDiskPath) {

        TableRegistry tableRegistry = runtimeC.getTableRegistry();
        CorfuTable<CorfuStoreMetadata.TableName,
                CorfuRecord<CorfuStoreMetadata.TableDescriptors,
                        CorfuStoreMetadata.TableMetadata>>
                        tableRegistryCT = tableRegistry.getRegistryTable();

        // Save the regular serializer first..
        ISerializer protobufSerializer = Serializers.getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtobufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtobufSerializer
        ISerializer dynamicProtobufSerializer = new DynamicProtobufSerializer(runtimeC);
        Serializers.registerSerializer(dynamicProtobufSerializer);

        // First checkpoint the TableRegistry system table
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();

        Token trimToken = new Token(Token.UNINITIALIZED.getEpoch(), Token.UNINITIALIZED.getSequence());
        for (CorfuStoreMetadata.TableName tableName : tableRegistry.listTables(null)) {
            String fullTableName = TableRegistry.getFullyQualifiedTableName(
                    tableName.getNamespace(), tableName.getTableName()
                    );
            SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder = runtimeC.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                    .setStreamName(fullTableName)
                    .setSerializer(dynamicProtobufSerializer);

            // Find out if a table needs to be backed up by disk path to even checkpoint
            boolean diskBased = tableRegistryCT.get(tableName).getMetadata().getDiskBased();
            if (diskBased) {
                final Path persistedCacheLocation = Paths.get(tempDiskPath + tableName.getTableName());
                final Options options = new Options().setCreateIfMissing(true);
                final Supplier<StreamingMap<CorfuDynamicKey, CorfuDynamicRecord>> mapSupplier = () -> new PersistedStreamingMap<>(
                        persistedCacheLocation, options,
                        dynamicProtobufSerializer, runtimeC);
                corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
            }

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());
            Token token = mcw.appendCheckpoints(runtimeC, "checkpointer");
            trimToken = Token.min(trimToken, token);
        }

        // Finally checkpoint the TableRegistry system table itself..
        mcw.addMap(tableRegistryCT);
        Token token = mcw.appendCheckpoints(runtimeC, "checkpointer");
        trimToken = Token.min(trimToken, token);

        if (!skipTrim) {
            runtimeC.getAddressSpaceView().prefixTrim(trimToken);
            runtimeC.getAddressSpaceView().gc();
        }
        // Lastly restore the regular protobuf serializer and undo the dynamic protobuf serializer
        // otherwise the test cannot continue beyond this point.
        Serializers.registerSerializer(protobufSerializer);
        return trimToken;
    }

    /**
     * This test is divided into 3 phases.
     * Phase 1: Writes data to CorfuStore in a disk backed table.
     * Phase 2: Checkpoint this table as a disk backed table & trim using a separate runtime.
     * Phase 3: Re-open the table to verify.
     */
    @Test
    public void checkpointDiskBasedUFO() throws Exception {

        final String namespace = "namespace";
        final String tableName = "table";
        final int numRecords = PARAMETERS.NUM_ITERATIONS_MODERATE;

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        String tempDiskPath = com.google.common.io.Files.createTempDir()
                .getAbsolutePath();
        final Table<Uuid, Uuid, ManagedResources> table = store.openTable(namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().persistentDataPath(Paths.get(tempDiskPath)).build());

        final long aLong = 1L;
        Uuid uuidVal = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
        ManagedResources metadata = ManagedResources.newBuilder()
                .setCreateTimestamp(aLong).build();
        for (int i = numRecords; i > 0; --i) {
            TxnContext tx = store.txn(namespace);
            Uuid uuidKey = Uuid.newBuilder().setMsb(aLong+i).setLsb(aLong+i).build();
            tx.putRecord(table, uuidKey, uuidVal, metadata);
            tx.commit();
        }
        final int TEN = 10;
        try (TxnContext txn = store.txn(namespace)) {
            Set<Uuid> keys = txn.keySet(tableName);
            Iterables.partition(keys, TEN);
            txn.commit();
        }

        runtime.shutdown();

        // PHASE 2 - compact it with another runtime like the compactor would!
        CorfuRuntime runtimeC = new CorfuRuntime(singleNodeEndpoint)
                .setCacheDisabled(true)
                .connect();
        checkpointAndTrimCorfuStore(runtimeC, false, tempDiskPath);
        runtimeC.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store2 = new CorfuStore(runtime);
        // PHASE 3 - verify that count is same after checkpoint and trim
        Table<Uuid, Uuid, ManagedResources> table2 = store2.openTable(namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().persistentDataPath(Paths.get(
                        com.google.common.io.Files.createTempDir().getAbsolutePath())).build());
        assertThat(table2.count()).isEqualTo(numRecords);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Proper way to handle a schema upgrade. Example of EventInfo table changing its key protobuf
     * 1. delete the table in the old version (On real upgrade this would entail a migration step)
     * 2. checkpoint and trim to wipe out old entries
     * 3. re-open the table with the new schema
     * 4. write data in the new shcema
     *
     * @throws Exception
     */
    @Test
    public void alterTableUsingDropTest() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        runtime = createRuntime(singleNodeEndpoint);

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(runtime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, SampleSchema.EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        SampleSchema.EventInfo value = SampleSchema.EventInfo.newBuilder().setName("simpleValue").build();

        long timestamp = System.currentTimeMillis();
        TxnContext tx = corfuStore.txn(nsxManager);
        tx.putRecord(table, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build());
        tx.commit();

        tx = corfuStore.txn(nsxManager);
        tx.putRecord(table, key, value,
                ManagedResources.newBuilder().setCreateUser("CreateUser").build());
        tx.commit();

        corfuStore.deleteTable(nsxManager, tableName);

        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        CorfuTable<Uuid, CorfuRecord<SampleSchema.EventInfo, ManagedResources>> corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Uuid, CorfuRecord<SampleSchema.EventInfo, ManagedResources>>>() {
                })
                .setStreamName(table.getFullyQualifiedTableName())
                .open();
        mcw.addMap(corfuTable);
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        Token trimPoint = mcw.appendCheckpoints(runtime, "checkpointer");
        runtime.getAddressSpaceView().prefixTrim(trimPoint);
        runtime.getAddressSpaceView().gc();
        runtime.getObjectsView().getObjectCache().clear();
        runtime.shutdown();
        runtime = createRuntime(singleNodeEndpoint);

        corfuStore = new CorfuStore(runtime);

        // Re-Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.EventInfo, SampleSchema.EventInfo, ManagedResources> tableV2 = corfuStore.openTable(
                nsxManager,
                tableName,
                SampleSchema.EventInfo.class, // Instead of using UUid using EventInfo as the key itself!
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        tx = corfuStore.txn(nsxManager);
        tx.putRecord(tableV2, value, value,
                ManagedResources.newBuilder().setCreateUser("CreateUser").build());
        tx.commit();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that tx.commit()---in the context of read-only transactions---returns the sequence
     * for the max tail of all streams accessed in the transaction. For the following scenarios:
     *
     * (1) Snapshot Transaction at an intermediate state (sequence = 7L) where the snapshot matches last update of one
     * of the streams of interest (streams accessed in the tx)
     * (2) Snapshot Transaction at an intermediate state, where the snapshot DOES NOT match last update of one
     * of the streams of interest (streams accessed in the tx) (sequence = 9L)
     * (3) Snapshot Transaction at latest state (sequence = 10L)
     *
     * In this test we write to 3 tables (T1, T2 & T3). Read-only transaction is performed across T1 & T2 at different points.
     * The following diagram illustrates the write pattern:
     *
     * +-----------------------------------------------------------+
     * | 0 |   1  |   2  |  3   | 4  |  5 |  6 |  7 |  8 |  9 | 10 |
     * +-----------------------------------------------------------+
     * | R | T1_R | T2_R | T3_R | T1 | T2 | T1 | T2 | T3 | T3 | T1 |
     * +-----------------------------------------------------------+
     *
     * R : Registry table registration
     * TX_R : TX registration in the Registry Table
     * TX : TX entry (update)
     *
     * @throws Exception
     */
    @Test
    public void testReadTransactionCommit() throws Exception {
        Process corfuServer = startCorfu();

        // Define a namespace for the table and table name
        final String namespace = "corfu-namespace";
        final String tableName1 = "Community-EventInfo";
        final String tableName2 = "Work-EventInfo";
        final String tableName3 = "Empty-EventInfo";
        final int numUpdates = 2;
        final long OFFSET = 4L;

        // Create & Register the table.
        Table<Uuid, SampleSchema.EventInfo, ManagedResources> table1 = corfuStore.openTable(
                namespace,
                tableName1,
                Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                TableOptions.builder().build());

       Table<Uuid, SampleSchema.EventInfo, ManagedResources> table2 = corfuStore.openTable(
                namespace,
                tableName2,
                Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                TableOptions.builder().build());

        Table<Uuid, SampleSchema.EventInfo, ManagedResources> table3 = corfuStore.openTable(
                namespace,
                tableName3,
                Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                TableOptions.builder().build());

        long offsetLog = OFFSET; // addresses 0, 1, 2, 3 are taken by updates to the Registry Table
        long partialSnapshot = offsetLog + (numUpdates*2) - 1;

        long maxGlobalAddress = generateUpdates(namespace, table1, table2, table3, offsetLog, numUpdates);

        // Start READ transaction on T1 & T2, at an intermediate SNAPSHOT (which matches exactly the last update of T1)
        try (TxnContext tx = corfuStore.txn(namespace,
                IsolationLevel.snapshot(CorfuStoreMetadata.Timestamp.newBuilder().setSequence(partialSnapshot).setEpoch(0L).build()))) {
            assertThat(tx.getTable(tableName1).count()).isEqualTo(numUpdates);
            assertThat(tx.getTable(tableName2).count()).isEqualTo(numUpdates);
            Timestamp readTx = tx.commit();
            assertThat(readTx.getSequence()).isEqualTo(partialSnapshot);
        }

        // Start READ transaction on T1 & T2, at a SNAPSHOT that does not match an update to any of the tables of interest
        try (TxnContext tx = corfuStore.txn(namespace,
                IsolationLevel.snapshot(CorfuStoreMetadata.Timestamp.newBuilder().setSequence(partialSnapshot + numUpdates).setEpoch(0L).build()))) {
            assertThat(tx.getTable(tableName1).count()).isEqualTo(numUpdates);
            assertThat(tx.getTable(tableName2).count()).isEqualTo(numUpdates);
            Timestamp readTx = tx.commit();
            assertThat(readTx.getSequence()).isEqualTo(partialSnapshot);
        }

        // Start READ transaction on T1 & T2 at the latest timestamp
        try (TxnContext tx = corfuStore.txn(namespace)) {
            assertThat(tx.getTable(tableName1).count()).isEqualTo(numUpdates + 1);
            assertThat(tx.getTable(tableName2).count()).isEqualTo(numUpdates);
            Timestamp readTx = tx.commit();
            assertThat(readTx.getSequence()).isEqualTo(maxGlobalAddress);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private long generateUpdates(String namespace, Table<Uuid, SampleSchema.EventInfo, ManagedResources> table1,
                                 Table<Uuid, SampleSchema.EventInfo, ManagedResources> table2,
                                 Table<Uuid, SampleSchema.EventInfo, ManagedResources> table3,
                                 long offsetLog, int numUpdates) {
        Uuid key;
        SampleSchema.EventInfo value;
        Timestamp sequenceNumber;
        long offsetCounter = offsetLog;

        // Generate updates to table1 and table2
        for (int i = 0; i < numUpdates; i++) {
            key = Uuid.newBuilder().setLsb(i).setMsb(i).build();
            value = SampleSchema.EventInfo.newBuilder().setName("simpleValue" + i).build();

            long timestamp = System.currentTimeMillis();
            try (TxnContext tx = corfuStore.txn(namespace)) {
                tx.putRecord(table1, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build());
                sequenceNumber = tx.commit();
                assertThat(sequenceNumber.getSequence()).isEqualTo(offsetCounter);
                offsetCounter++;
            }

            try (TxnContext tx = corfuStore.txn(namespace)) {
                tx.putRecord(table2, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build());
                sequenceNumber = tx.commit();
                assertThat(sequenceNumber.getSequence()).isEqualTo(offsetCounter);
                offsetCounter++;
            }
        }

        // Generate updates to table3 (such that there are unrelated updates of the read TX in between the next update to table1)
        for (int i = 0; i < numUpdates; i++) {
            key = Uuid.newBuilder().setLsb(i).setMsb(i).build();
            value = SampleSchema.EventInfo.newBuilder().setName("simpleValue" + i).build();

            long timestamp = System.currentTimeMillis();
            try (TxnContext tx = corfuStore.txn(namespace)) {
                tx.putRecord(table3, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build());
                sequenceNumber = tx.commit();
                assertThat(sequenceNumber.getSequence()).isEqualTo(offsetCounter);
                offsetCounter++;
            }
        }

        // Generate last update to table1 (such that at least one of the tables of interest
        // has an update after other unrelated updates)
        long timestamp = System.currentTimeMillis();
        try (TxnContext tx = corfuStore.txn(namespace)) {
            key = Uuid.newBuilder().setLsb(offsetCounter).setMsb(offsetCounter).build();
            value = SampleSchema.EventInfo.newBuilder().setName("simpleValue" + offsetCounter).build();
            tx.putRecord(table1, key, value,
                    ManagedResources.newBuilder()
                            .setCreateTimestamp(timestamp).build());
            sequenceNumber = tx.commit();
            assertThat(sequenceNumber.getSequence()).isEqualTo(offsetCounter);
        }

        return offsetCounter;
    }

    private Process startCorfu() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        runtime = createRuntime(singleNodeEndpoint);
        corfuStore = new CorfuStore(runtime);

        return corfuServer;
    }
}
