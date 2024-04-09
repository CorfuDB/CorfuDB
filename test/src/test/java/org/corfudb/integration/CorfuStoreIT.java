package org.corfudb.integration;

import com.google.common.collect.Iterables;
import com.google.protobuf.DynamicMessage;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.CompactorLeaderServices;
import org.corfudb.infrastructure.LivenessValidator;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.ExampleSchemas.ContactBookId;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.IsolationLevel;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableParameters;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.TableRegistry.TableDescriptor;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedCorfuTableGenericConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableConfig.ManagedDynamicProtobufSerializer;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedRuntime;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.runtime.view.ObjectsView.LOG_REPLICATOR_STREAM_INFO;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.FQ_PROTO_DESC_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.FQ_REGISTRY_TABLE_NAME;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Simple test that inserts data into CorfuStore via a separate server process
 */
@Slf4j
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

    private Process runSinglePersistentServer(String host, int port, boolean disableLogUnitServerCache) throws IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .setDisableLogUnitServerCache(disableLogUnitServerCache)
                .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * This test is divided into 3 phases.
     * Phase 1: Writes data to CorfuStore in a Table using the transaction builder.
     * Phase 2: Using DynamicMessages, we try to read and edit the message.
     * Phase 3: Using the corfuStore the message is read back to ensure, the schema wasn't altered and the
     * serialization isn't broken. The 'lsb' value of the metadata is asserted.
     */
    @Test
    public void readDataWithDynamicMessages() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        FullyQualifiedTableName fqTableName = FullyQualifiedTableName.build("namespace", "table");
        final long keyUuid = 1L;
        final long valueUuid = 3L;

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

        buildManagedRuntime().connect(rt -> {
            CorfuStore store = new CorfuStore(rt);
            Table<Uuid, Uuid, ManagedResources> table = store.openTable(
                    fqTableName,
                    new TableDescriptor<>(Uuid.class, Uuid.class, ManagedResources.class),
                    TableOptions.fromProtoSchema(Uuid.class)
            );

            TxnContext tx = store.txn(fqTableName.rawNamespace());
            tx.putRecord(table, uuidKey, uuidVal, metadata);
            tx.commit();
        });


        final long newMetadataUuid = 99L;
        ManagedRuntime managedRt = buildManagedRuntime();

        ManagedCorfuTableGenericConfig<CorfuDynamicKey, CorfuDynamicRecord> config = ManagedCorfuTableGenericConfig
                .<CorfuDynamicKey, CorfuDynamicRecord>builder()
                .managedSerializer(new ManagedDynamicProtobufSerializer())
                .build();

        ManagedCorfuTable<CorfuDynamicKey, CorfuDynamicRecord> managedTable = ManagedCorfuTable
                .<CorfuDynamicKey, CorfuDynamicRecord>build()
                .config(config)
                .managedRt(managedRt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentProtobufCorfu());

        managedTable.execute(ctx -> {
            // PHASE 2
            // Interpret using dynamic messages.
            CorfuRuntime runtime = ctx.getRt();
            GenericCorfuTable<?, CorfuDynamicKey, CorfuDynamicRecord> corfuTable = ctx.getCorfuTable();

            corfuTable.entryStream().forEach(entry -> {
                CorfuDynamicKey key = entry.getKey();
                CorfuDynamicRecord value = entry.getValue();
                DynamicMessage metaMsg = value.getMetadata();

                DynamicMessage.Builder newMetaBuilder = metaMsg.toBuilder();
                metaMsg.getAllFields().forEach((fieldDescriptor, o) -> {
                    if (fieldDescriptor.getName().equals("create_timestamp")) {
                        newMetaBuilder.setField(fieldDescriptor, newMetadataUuid);
                    }
                });

                CorfuDynamicRecord record = new CorfuDynamicRecord(
                        value.getPayloadTypeUrl(),
                        value.getPayload(),
                        value.getMetadataTypeUrl(),
                        newMetaBuilder.build()
                );
                corfuTable.insert(key, record);
            });

            assertThat(corfuTable.size()).isEqualTo(1);

            PersistentCorfuTable<CorfuDynamicKey, CorfuDynamicRecord> tableRegistry = runtime
                    .getObjectsView()
                    .build()
                    .setTypeToken(PersistentCorfuTable.<CorfuDynamicKey, CorfuDynamicRecord>getTypeToken())
                    .setStreamName(FQ_REGISTRY_TABLE_NAME.toFqdn())
                    .setSerializer(config.getSerializer(runtime))
                    .addOpenOption(ObjectOpenOption.NO_CACHE)
                    .open();

            PersistentCorfuTable<CorfuDynamicKey, CorfuDynamicRecord> descriptorTable = runtime
                    .getObjectsView()
                    .build()
                    .setTypeToken(PersistentCorfuTable.<CorfuDynamicKey, CorfuDynamicRecord>getTypeToken())
                    .setStreamName(FQ_PROTO_DESC_TABLE_NAME.toFqdn())
                    .setSerializer(config.getSerializer(runtime))
                    .addOpenOption(ObjectOpenOption.NO_CACHE)
                    .open();

            MultiCheckpointWriter<GenericCorfuTable<?, CorfuDynamicKey, CorfuDynamicRecord>> mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTable);
            mcw.addMap(tableRegistry);
            mcw.addMap(descriptorTable);
            Token trimPoint = mcw.appendCheckpoints(runtime, "checkpointer");

            runtime.getAddressSpaceView().prefixTrim(trimPoint);
            runtime.getAddressSpaceView().gc();
            runtime.getSerializers().clearCustomSerializers();
        });


        buildManagedRuntime().connect(runtime -> {
            // PHASE 3
            // Read using protobuf serializer.
            runtime = createRuntime(singleNodeEndpoint);
            CorfuStore store3 = new CorfuStore(runtime);

            // Attempting to open an unopened table with the short form should throw the IllegalArgumentException
            assertThatThrownBy(() -> store3.getTable(fqTableName)).
                    isExactlyInstanceOf(IllegalArgumentException.class);

            // Attempting to open a non-existent table should throw NoSuchElementException
            FullyQualifiedTableName invalidTableName = FullyQualifiedTableName
                    .build(fqTableName.rawNamespace(), "NonExistingTableName");

            assertThatThrownBy(() -> store3.getTable(invalidTableName))
                    .isExactlyInstanceOf(NoSuchElementException.class);

            Table<Uuid, Uuid, ManagedResources> table1 = store3.openTable(
                    fqTableName,
                    new TableDescriptor<>(Uuid.class, Uuid.class, ManagedResources.class),
                    TableOptions.fromProtoSchema(Uuid.class)
            );

            try (TxnContext txn = store3.txn(fqTableName.rawNamespace())) {
                CorfuStoreEntry<Uuid, Uuid, ManagedResources> record = txn.getRecord(fqTableName.rawTableName(), uuidKey);
                assertThat(record.getMetadata().getCreateTimestamp()).isEqualTo(newMetadataUuid);
                txn.putRecord(table1, uuidKey, uuidVal, metadata);
                txn.commit();
            }

            assertThat(shutdownCorfuServer(corfuServer)).isTrue();
        });
    }

    private ManagedRuntime buildManagedRuntime() {
        return ManagedRuntime
                .withCacheDisabled()
                .setup(rt -> rt.parseConfigurationString(singleNodeEndpoint));
    }

    public Token checkpointAndTrimCorfuStore(CorfuRuntime runtimeC, boolean skipTrim, String tempDiskPath) {

        // open these metadata system tables in normal Protobuf serializer since we know their types!
        TableRegistry tableRegistry = runtimeC.getTableRegistry();
        PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>
                tableRegistryCT = tableRegistry.getRegistryTable();

        PersistentCorfuTable<CorfuStoreMetadata.ProtobufFileName,
                CorfuRecord<ProtobufFileDescriptor, TableMetadata>>
                descriptorTableCT = tableRegistry.getProtobufDescriptorTable();

        Token trimToken = new Token(Token.UNINITIALIZED.getEpoch(), Token.UNINITIALIZED.getSequence());

        // First checkpoint the TableRegistry & Descriptor system tables because they get opened
        // BEFORE any DynamicProtobufSerializer and CorfuDynamicMessage kicks in
        MultiCheckpointWriter<ICorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        // First checkpoint the TableRegistry and Descriptor system tables..
        mcw.addMap(tableRegistryCT);
        mcw.addMap(descriptorTableCT);
        Token token = mcw.appendCheckpoints(runtimeC, "checkpointer");
        trimToken = Token.min(trimToken, token);

        // Save the regular serializer first..
        ISerializer protobufSerializer = runtimeC.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        // Must register dynamicProtobufSerializer *AFTER* the getTableRegistry() call to ensure that
        // the serializer does not go back to the regular ProtobufSerializer
        ISerializer dynamicProtobufSerializer = new DynamicProtobufSerializer(runtimeC);
        runtimeC.getSerializers().registerSerializer(dynamicProtobufSerializer);

        for (TableName tableName : tableRegistry.listTables(null)) {
            FullyQualifiedTableName fullTableName = FullyQualifiedTableName.build(tableName);

            if (fullTableName.isSystemNsAndTable()) {
                continue; // these tables should have already been checkpointed using normal serializer!
            }

            SMRObject.Builder<? extends ICorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder =
                    runtimeC.getObjectsView().build()
                            .setTypeToken(PersistentCorfuTable.<CorfuDynamicKey, CorfuDynamicRecord>getTypeToken())
                            .setStreamName(fullTableName.toFqdn())
                            .setSerializer(dynamicProtobufSerializer);

            // Find out if a table needs to be backed up by disk path to even checkpoint
            boolean diskBased = tableRegistryCT.get(tableName).getMetadata().getDiskBased();
            if (diskBased) {
                final PersistenceOptions persistenceOptions = PersistenceOptions.builder()
                        .dataPath(Paths.get(tempDiskPath + tableName.getTableName()))
                        .consistencyModel(CorfuOptions.ConsistencyModel.READ_YOUR_WRITES)
                        .build();
                corfuTableBuilder.setArguments()
                        .setTypeToken(PersistedCorfuTable.getTypeToken())
                        .setArguments(persistenceOptions, dynamicProtobufSerializer);
            }

            mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTableBuilder.open());
            Token tableToken = mcw.appendCheckpoints(runtimeC, "checkpointer");
            trimToken = Token.min(trimToken, tableToken);
        }

        if (!skipTrim) {
            runtimeC.getAddressSpaceView().prefixTrim(trimToken);
            runtimeC.getAddressSpaceView().gc();
        }
        // Lastly restore the regular protobuf serializer and undo the dynamic protobuf serializer
        // otherwise the test cannot continue beyond this point.
        runtimeC.getSerializers().registerSerializer(protobufSerializer);
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
        FullyQualifiedTableName fqTableName = FullyQualifiedTableName.build("namespace", "table");
        final int numRecords = PARAMETERS.NUM_ITERATIONS_MODERATE;

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        Path tempDiskPath = Files.createTempDirectory(fqTableName.toFqdn());
        final CorfuOptions.PersistenceOptions persistenceOptions = CorfuOptions.PersistenceOptions.newBuilder()
                .setDataPath(tempDiskPath.toAbsolutePath().toString()).build();

        final Table<Uuid, Uuid, ManagedResources> table = store.openTable(
                fqTableName,
                new TableDescriptor<>(Uuid.class, Uuid.class, ManagedResources.class),
                TableOptions.builder().persistenceOptions(persistenceOptions).build()
        );

        String namespace = fqTableName.rawNamespace();
        final long aLong = 1L;
        Uuid uuidVal = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
        ManagedResources metadata = ManagedResources.newBuilder()
                .setCreateTimestamp(aLong).build();
        for (int i = numRecords; i > 0; --i) {
            TxnContext tx = store.txn(namespace);
            Uuid uuidKey = Uuid.newBuilder().setMsb(aLong + i).setLsb(aLong + i).build();
            tx.putRecord(table, uuidKey, uuidVal, metadata);
            tx.commit();
        }
        final int TEN = 10;
        try (TxnContext txn = store.txn(namespace)) {
            Set<Uuid> keys = txn.entryStream(table).map(CorfuStoreEntry::getKey).collect(Collectors.toSet());
            Iterables.partition(keys, TEN);
            txn.commit();
        }

        runtime.shutdown();

        // PHASE 2 - compact it with another runtime like the compactor would!
        CorfuRuntime runtimeC = new CorfuRuntime(singleNodeEndpoint)
                .setCacheDisabled(true)
                .connect();
        checkpointAndTrimCorfuStore(runtimeC, false, tempDiskPath.toAbsolutePath().toString());
        runtimeC.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store2 = new CorfuStore(runtime);
        // PHASE 3 - verify that count is same after checkpoint and trim
        TableOptions tableOptions = TableOptions.builder()
                .persistentDataPath(Files.createTempDirectory("checkpointDiskBasedUFO"))
                .build();

        Table<Uuid, Uuid, ManagedResources> table2 = store2.openTable(
                fqTableName,
                new TableDescriptor<>(Uuid.class, Uuid.class, ManagedResources.class),
                tableOptions
        );

        assertThat(table2.count()).isEqualTo(numRecords);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }


    /**
     * Verify concurrent execution on the same snapshot for
     * disk-backed Corfu. This test is probabilistic in nature.
     *
     * @throws IOException during Corfu Server startup or Table open
     */
    @Test
    @Ignore("Failing after JDK-11, this test will be fixed in future patches.")
    public void concurrentExecuteQueryDiskBacked() throws Exception {
        final String namespace = "test-namespace";
        final String tableName = "test-table";
        final int numRecords = PARAMETERS.NUM_ITERATIONS_MODERATE;

        final Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        Path tempDiskPath = Files.createTempDirectory(tableName);
        final Table<Uuid, Uuid, ManagedResources> diskBackedTable2 = store.openTable(
                namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().persistenceOptions(CorfuOptions.PersistenceOptions.newBuilder()
                                .setDataPath(tempDiskPath.toAbsolutePath().toString())
                                .setConsistencyModel(CorfuOptions.ConsistencyModel.READ_YOUR_WRITES).build())
                        .build());

        List<Uuid> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add(Uuid.newBuilder().setMsb(i).build());
        }

        records.forEach(entry -> {
            try (TxnContext tx = store.txn(namespace)) {
                tx.putRecord(diskBackedTable2, entry, entry, null);
                tx.commit();
            }
        });

        records.parallelStream().forEach(record -> {
            try (TxnContext tx = store.txn(namespace)) {
                assertThat(tx.executeQuery(diskBackedTable2, entry -> entry.getPayload().equals(record)).size())
                        .isEqualTo(1);
            }
        });
    }

    /**
     * Proper way to handle a schema upgrade. Example of EventInfo table changing its key protobuf
     * 1. delete the table in the old version (On real upgrade this would entail a migration step)
     * 2. checkpoint and trim to wipe out old entries
     * 3. re-open the table with the new schema
     * 4. write data in the new shcema
     *
     * @throws Exception exception
     */
    @Test
    public void alterTableUsingDropTest() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

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

        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        PersistentCorfuTable<Uuid, CorfuRecord<SampleSchema.EventInfo, ManagedResources>> corfuTable =
                createCorfuTable(runtime, table.getFullyQualifiedTableName());

        mcw.addMap(corfuTable);
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
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
     * This test verifies that syncing a table with entries different from the schema it was
     * opened with does not throw an error if the ProtobufSerializer is made aware of the
     * different schema.
     *
     * @throws Exception exception
     */
    @Test
    public void syncTableWithOldSchemaTest() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(runtime);
        FullyQualifiedTableName fqTableName = FullyQualifiedTableName.build("some-namespace", "EventInfo");

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, SampleSchema.EventInfo, ManagedResources> table = corfuStore.openTable(
                fqTableName,
                new TableDescriptor<>(Uuid.class, SampleSchema.EventInfo.class, ManagedResources.class),
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();

        /*
         * HACK - PLEASE DO NOT TRY THIS AT HOME -
         * Create a fake Table bypassing the TableRegistry but with the same table name to insert
         * records of the wrong type (ContactBookId) into a table which was opened with a different schema
         * (EventInfo)
         */
        ContactBookId defaultValueMessage = (ContactBookId) ContactBookId.class.getMethod("getDefaultInstance").invoke(null);
        ManagedResources defaultMetadataMessage = (ManagedResources) ManagedResources
                .class.getMethod("getDefaultInstance").invoke(null);
        CorfuOptions.PersistenceOptions persistenceOptions = CorfuOptions.PersistenceOptions.newBuilder().build();

        Table<Uuid, ContactBookId, ManagedResources> badTable = new Table<>(
                TableParameters.<Uuid, ContactBookId, ManagedResources>builder()
                        .namespace(fqTableName.rawNamespace())
                        .fullyQualifiedTableName(fqTableName.toFqdn())
                        .kClass(Uuid.class)
                        .vClass(ContactBookId.class)
                        .mClass(ManagedResources.class)
                        .valueSchema(defaultValueMessage)
                        .metadataSchema(defaultMetadataMessage)
                        .schemaOptions(CorfuOptions.SchemaOptions.getDefaultInstance())
                        .secondaryIndexesDisabled(true)
                        .persistenceOptions(persistenceOptions)
                        .build(),
                runtime,
                runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE),
                new HashSet<>(Collections.singletonList(LOG_REPLICATOR_STREAM_INFO.getStreamId())));

        // Now this badTable is completely hidden from both the TableRegistry and the Serializer!
        TxnContext tx = corfuStore.txn(fqTableName.rawNamespace());
        long timestamp = System.currentTimeMillis();
        ContactBookId badSchema = ContactBookId.newBuilder()
                .setName(fqTableName.rawTableName()).build();
        tx.putRecord(badTable, key, badSchema,
                ManagedResources.newBuilder()
                        .setCreateTimestamp(timestamp).build());
        tx.commit();

        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        corfuStore = new CorfuStore(runtime);

        // Re-open the table with the same schema and test if it hits a serializer exception
        // as it would when a new code is trying to open something from an older instance
        Table<Uuid, SampleSchema.EventInfo, ManagedResources> tableV2 = corfuStore.openTable(
                fqTableName,
                new TableDescriptor<>(Uuid.class, SampleSchema.EventInfo.class, ManagedResources.class),
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        boolean didAssertFire = false;
        tx = corfuStore.txn(fqTableName.rawNamespace());
        try {
            tx.clear(tableV2);
            tx.commit();
        } catch (TransactionAbortedException e) {
            assertThat(e.getAbortCause()).isEqualTo(AbortCause.UNDEFINED);
            didAssertFire = true;
        }
        assertThat(didAssertFire).isTrue();
        // Once the unknown type is added to the map it should be ok to clear the table
        runtime.getTableRegistry().addTypeToClassMap(ContactBookId.getDefaultInstance());

        tx = corfuStore.txn(fqTableName.rawNamespace());
        tx.clear(tableV2);
        tx.commit();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test that tx.commit()---in the context of read-only transactions---returns the sequence
     * for the max tail of all streams accessed in the transaction. For the following scenarios:
     * <p>
     * (1) Snapshot Transaction at an intermediate state (sequence = 7L) where the snapshot matches last update of one
     * of the streams of interest (streams accessed in the tx)
     * (2) Snapshot Transaction at an intermediate state, where the snapshot DOES NOT match last update of one
     * of the streams of interest (streams accessed in the tx) (sequence = 9L)
     * (3) Snapshot Transaction at latest state (sequence = 10L)
     * <p>
     * In this test we write to 3 tables (T1, T2 & T3). Read-only transaction is performed across T1 & T2 at different points.
     * The following diagram illustrates the write pattern:
     * <p>
     * +-----------------------------------------------------------+
     * | 0 |   1  |   2  |  3   | 4  |  5 |  6 |  7 |  8 |  9 | 10 |
     * +-----------------------------------------------------------+
     * | R | T1_R | T2_R | T3_R | T1 | T2 | T1 | T2 | T3 | T3 | T1 |
     * +-----------------------------------------------------------+
     * <p>
     * R : Registry table registration
     * TX_R : TX registration in the Registry Table
     * TX : TX entry (update)
     *
     * @throws Exception exception
     */
    @Test
    public void testReadTransactionCommit() throws Exception {
        Process corfuServer = startCorfu();

        // Define a namespace for the table and table name
        final String namespace = "corfu-namespace";
        final String tableName1 = "Community-EventInfo";
        final String tableName2 = "Work-EventInfo";
        final String tableName3 = "Empty-EventInfo";
        final String tableNameNoWrites = "NoWritesTable";
        final int numUpdates = 2;
        final long OFFSET = 5L;

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

        // addresses 0, 1, 2, 3, 4 are taken by updates to the Registry Table & Descriptors
        long offsetLog = OFFSET;
        long partialSnapshot = offsetLog + (numUpdates * 2) - 1;

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

        corfuStore.openTable(
                namespace,
                tableNameNoWrites,
                Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                TableOptions.builder().build());

        // Start READ tx on a table never written to (to confirm the read timestamp corresponds to init of the log)
        try (TxnContext tx = corfuStore.txn(namespace)) {
            assertThat(tx.getTable(tableNameNoWrites).count()).isEqualTo(0);
            Timestamp readTx = tx.commit();
            assertThat(readTx.getSequence()).isPositive();
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
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        corfuStore = new CorfuStore(runtime);

        return corfuServer;
    }

    /**
     * Verify that LogData metadata information is recovered when entries are no longer
     * available in the server's cache and are read back from disk. Particularly, test
     * for the presence of 'epoch' which was missing (until this fix).
     *
     * @throws Exception exception
     */
    @Test
    public void testLogDataPersistedMetadata() throws Exception {
        Process corfuServer = null;
        try {
            corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
            FullyQualifiedTableName fqTableName = FullyQualifiedTableName.build("namespace", "table");
            Timestamp commitTimestamp;

            // Start a Corfu runtime & Corfu Store
            CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
            CorfuStore store = new CorfuStore(runtime);

            // Open one table and write one update
            final Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.ManagedMetadata> tableA = store.openTable(
                    fqTableName,
                    new TableDescriptor<>(SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class, SampleSchema.ManagedMetadata.class),
                    TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class)
            );

            SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
            SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder().setPayload("Payload Value").build();
            SampleSchema.ManagedMetadata metadata = SampleSchema.ManagedMetadata.newBuilder().setCreateTime(System.currentTimeMillis())
                    .setCreateUser("User_IT").build();

            try (TxnContext tx = store.txn(fqTableName.rawNamespace())) {
                tx.putRecord(tableA, key, value, metadata);
                commitTimestamp = tx.commit();
            }

            // Invalidate server cache, so data is loaded from disk (re-built),
            // here was the previous bug as LogEntry did not persist the epoch
            // and hence it was lost when data was read from disk
            runtime.getAddressSpaceView().invalidateServerCaches();

            // Bypass VLO/MVO and do a direct read so we can access the LogData
            ILogData ld = runtime.getAddressSpaceView().read(commitTimestamp.getSequence());

            // TODO: this should match the commitTs epoch, but we have a bug and it returns -1 when no access is done in the TX
            assertThat(runtime.getLayoutView().getLayout().getEpoch()).isEqualTo(ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.EPOCH));
            assertThat(commitTimestamp.getSequence()).isEqualTo(ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.GLOBAL_ADDRESS));
            assertThat(Thread.currentThread().getId()).isEqualTo(ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.THREAD_ID));
            assertThat(runtime.getParameters().getCodecType()).isEqualTo(ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.PAYLOAD_CODEC));
            assertThat(runtime.getParameters().getClientId()).isEqualTo(ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.CLIENT_ID));
            Map<UUID, Long> backpointerMap = (Map<UUID, Long>) ld.getMetadataMap().get(IMetadata.LogUnitMetadataType.BACKPOINTER_MAP);
            Set<UUID> expectedTableUpdates = new HashSet<>();
            // Table itself (tableA)
            expectedTableUpdates.add(fqTableName.toStreamId().getId());
            // Stream tags for tableA
            expectedTableUpdates.add(TableRegistry.getStreamIdForStreamTag(fqTableName.rawNamespace(), "sample_streamer_1"));
            expectedTableUpdates.add(TableRegistry.getStreamIdForStreamTag(fqTableName.rawNamespace(), "sample_streamer_2"));
            // This is a federated table so add Log Replicator Stream Id
            expectedTableUpdates.add(ObjectsView.getLogReplicatorStreamId());
            assertThat(expectedTableUpdates.size()).isEqualTo(backpointerMap.size());
            expectedTableUpdates.forEach(table -> assertThat(backpointerMap.keySet()).contains(table));
        } finally {
            if (corfuServer != null) {
                shutdownCorfuServer(corfuServer);
            }
        }
    }

    private static final String NAMESPACE = "some-namespace";
    private static final String UNUSED_TABLE_NAME = "some-unused-table";

    /**
     * This test validates the computation of the trim token during initCompactionCycle.
     * Specifically, we validate that the trim token computed does not include addresses
     * that can correspond to new tables that have been registered after listTables is
     * invoked by the compactor.
     */
    @Test
    public void validateTrimTokenListTableRace() throws Exception {
        final Process p = new AbstractIT.CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setLogPath(getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .setSingle(true)
                .runServer();

        final CorfuRuntime clientRuntime = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        final CorfuRuntime cpRuntime = new CorfuRuntime(DEFAULT_ENDPOINT).connect();

        final CorfuStore clientStore = new CorfuStore(clientRuntime);
        final CorfuStore cpStoreSpy = spy(new CorfuStore(cpRuntime));

        final CountDownLatch listTableLatch = new CountDownLatch(1);
        final CountDownLatch writeLatch = new CountDownLatch(1);
        AtomicLong safeTrimPoint = new AtomicLong();
        AtomicBoolean encounteredException = new AtomicBoolean(false);

        Thread compactionThread = new Thread(() -> {
            try {
                // Initializing CompactorLeaderServices will open and register compactor metadata tables.
                // This will include writing entries to the RegistryTable.
                final CompactorLeaderServices leaderServices = new CompactorLeaderServices(cpRuntime,
                        DEFAULT_ENDPOINT, cpStoreSpy, new LivenessValidator(
                        cpRuntime, cpStoreSpy, Duration.ofMinutes(1)));

                // Store the log tail at this time. The trim token computed by initCompactionCycle
                // should not exceed this value.
                safeTrimPoint.set(clientRuntime.getAddressSpaceView().getLogTail());

                doAnswer(invocationOnMock -> {
                    Collection<TableName> tableNames = cpRuntime.getTableRegistry().listTables();
                    listTableLatch.countDown();
                    writeLatch.await();
                    return tableNames;
                }).when(cpStoreSpy).listTables(isNull(String.class));

                leaderServices.initCompactionCycle();
            } catch (Exception ex) {
                ex.printStackTrace();
                encounteredException.set(true);
            }
        });

        Thread writerThread = new Thread(() -> {
            try {
                // Wait until the compactor thread had performed listTables.
                listTableLatch.await();

                // Open a previously unused table. The registration process writes to the RegistryTable.
                clientStore.openTable(NAMESPACE, UNUSED_TABLE_NAME, RpcCommon.UuidMsg.class,
                        RpcCommon.UuidMsg.class, null, TableOptions.builder().build());

                // Notify the compactor thread to proceed with computing the trim token.
                writeLatch.countDown();
            } catch (Exception ex) {
                ex.printStackTrace();
                encounteredException.set(true);
            }
        });

        writerThread.start();
        compactionThread.start();
        writerThread.join();
        compactionThread.join();

        // Validate that no exceptions were hit by either thread.
        assertThat(encounteredException.get()).isFalse();

        // Validate the trim token.
        RpcCommon.TokenMsg trimToken;

        try (TxnContext tx = cpStoreSpy.txn(CORFU_SYSTEM_NAMESPACE)) {
            trimToken = (RpcCommon.TokenMsg) tx.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.MIN_CHECKPOINT).getPayload();
            tx.commit();
        }

        assertThat(trimToken.getSequence()).isLessThanOrEqualTo(safeTrimPoint.get());

        clientRuntime.shutdown();
        cpRuntime.shutdown();
        shutdownCorfuServer(p);
    }

    @Test
    public void resetTrimmedTableTest() throws Exception {
        final Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort, true);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore corfuStore = new CorfuStore(runtime);

        final String namespace = "test-namespace";
        final String tableNameA = "test-table-a";
        final String tableNameB = "test-table-b";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, Uuid, Uuid> tableA = corfuStore.openTable(
                namespace,
                tableNameA,
                Uuid.class,
                Uuid.class,
                null,
                TableOptions.builder().build());
        Table<Uuid, Uuid, Uuid> tableB = corfuStore.openTable(
                namespace,
                tableNameB,
                Uuid.class,
                Uuid.class,
                null,
                TableOptions.builder().build());

        Uuid key1 = Uuid.newBuilder().setLsb(1L).setMsb(1L).build();
        Uuid key2 = Uuid.newBuilder().setLsb(2L).setMsb(2L).build();

        TxnContext tx = corfuStore.txn(namespace);
        tx.putRecord(tableA, key1, key1, null);
        tx.commit();

        tx = corfuStore.txn(namespace);
        tx.putRecord(tableB, key2, key2, null);
        tx.commit();

        // Unregister table B and invoke checkpointer
        unregisterTable(runtime, namespace, tableNameB);
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        PersistentCorfuTable<Uuid, CorfuRecord<Uuid, Uuid>> corfuTableA =
                createCorfuTable(runtime, tableA.getFullyQualifiedTableName());
        mcw.addMap(corfuTableA); //Checkpoint only table A
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
        Token trimPoint = mcw.appendCheckpoints(runtime, "checkpointer");
        runtime.getAddressSpaceView().prefixTrim(trimPoint);

        //Re-open table B
        corfuStore.openTable(
                namespace,
                tableNameB,
                Uuid.class,
                Uuid.class,
                null,
                TableOptions.builder().build());

        tx = corfuStore.txn(namespace);
        assertThat(tx.getRecord(tableNameA, key1).getPayload()).isEqualTo(key1);
        assertThat(tx.getRecord(tableNameB, key2).getPayload()).isNull();
        tx.commit();
        runtime.shutdown();
        shutdownCorfuServer(corfuServer);
    }

    /**
     * This test is divided into 2 phases.
     * Phase 1: Open a table with metadata A but write record with metadata B
     * Phase 2: Use a new runtime and open with metadata A and verify deserialization exception
     */
    @Test
    public void badMetadataDeserializationTest() throws Exception {
        final String namespace = "namespace";
        final String tableName = "table";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // PHASE 1 - Start a Corfu runtime & a CorfuStore instance
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<Uuid, Uuid, Uuid> table = store.openTable(namespace, tableName,
                Uuid.class, Uuid.class, Uuid.class,
                TableOptions.fromProtoSchema(Uuid.class));

        final long aLong = 1L;
        Uuid uuidVal = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
        try (TxnContext tx = store.txn(namespace)) {
            Uuid uuidKey = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
            tx.putRecord(table, uuidKey, uuidVal, uuidVal);
            tx.commit();
        }

        // Stunts performed by experts please don't try this at home.
        final Table<Uuid, Uuid, ManagedResources> tableBad = store.openTable(namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        ManagedResources metadata = ManagedResources.newBuilder()
                .setCreateTimestamp(aLong).build();
        try (TxnContext tx = store.txn(namespace)) {
            Uuid uuidKey = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
            tx.putRecord(tableBad, uuidKey, uuidVal, metadata);
            tx.commit();
        }
        runtime.shutdown();

        // PHASE 2 - open the same table this time only with one metadata type
        CorfuRuntime runtimeC = new CorfuRuntime(singleNodeEndpoint)
                .setCacheDisabled(true)
                .connect();
        store = new CorfuStore(runtimeC);
        final Table<Uuid, Uuid, Uuid> tableVictim = store.openTable(namespace, tableName,
                Uuid.class, Uuid.class, Uuid.class,
                TableOptions.fromProtoSchema(Uuid.class));
        Exception whichException = null;
        try {
            tableVictim.count();
        } catch (Exception ex) {
            whichException = ex;
        }
        assertThat(whichException).isExactlyInstanceOf(SerializerException.class);
        assertThat(whichException.getMessage()).contains("ManagedResource");
        runtimeC.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    @Test
    public void resetTrimmedTableInParallelTest() throws Exception {
        final Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort, true);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore corfuStore = new CorfuStore(runtime);

        final String namespace = "test-namespace";
        final String tableNameA = "test-table-a";
        final String tableNameB = "test-table-b";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, Uuid, Uuid> tableA = corfuStore.openTable(
                namespace,
                tableNameA,
                Uuid.class,
                Uuid.class,
                null,
                TableOptions.builder().build());
        Table<Uuid, Uuid, Uuid> tableB = corfuStore.openTable(
                namespace,
                tableNameB,
                Uuid.class,
                Uuid.class,
                null,
                TableOptions.builder().build());

        Uuid key1 = Uuid.newBuilder().setLsb(1L).setMsb(1L).build();
        Uuid key2 = Uuid.newBuilder().setLsb(2L).setMsb(2L).build();

        TxnContext tx = corfuStore.txn(namespace);
        tx.putRecord(tableA, key1, key1, null);
        tx.commit();

        tx = corfuStore.txn(namespace);
        tx.putRecord(tableB, key2, key2, null);
        tx.commit();

        // Unregister table B and invoke checkpointer
        unregisterTable(runtime, namespace, tableNameB);
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        PersistentCorfuTable<Uuid, CorfuRecord<Uuid, Uuid>> corfuTableA =
                createCorfuTable(runtime, tableA.getFullyQualifiedTableName());
        mcw.addMap(corfuTableA); //Checkpoint only table A
        mcw.addMap(runtime.getTableRegistry().getRegistryTable());
        mcw.addMap(runtime.getTableRegistry().getProtobufDescriptorTable());
        Token trimPoint = mcw.appendCheckpoints(runtime, "checkpointer");
        runtime.getAddressSpaceView().prefixTrim(trimPoint);

        CorfuRuntime runtime2 = createRuntime(singleNodeEndpoint);
        CorfuStore corfuStore2 = new CorfuStore(runtime2);

        ExecutorService scheduler = Executors.newFixedThreadPool(2);
        scheduler.execute(() -> {
            try {
                corfuStore.openTable(
                        namespace,
                        tableNameB,
                        Uuid.class,
                        Uuid.class,
                        null,
                        TableOptions.builder().build());
                TxnContext tx1 = corfuStore.txn(namespace);
                tx1.putRecord(tableB, key2, key2, null);
                tx1.commit();
            } catch (Exception e) {
                log.error("Exception thrown while opening table");
            }
        });
        Uuid key3 = Uuid.newBuilder().setLsb(3L).setMsb(3L).build();
        scheduler.execute(() -> {
            try {
                corfuStore2.openTable(
                        namespace,
                        tableNameB,
                        Uuid.class,
                        Uuid.class,
                        null,
                        TableOptions.builder().build());
                TxnContext tx2 = corfuStore.txn(namespace);
                tx2.putRecord(tableB, key3, key3, null);
                tx2.commit();
            } catch (Exception e) {
                log.error("Exception thrown while opening table");
            }
        });

        scheduler.awaitTermination(10, TimeUnit.SECONDS);

        tx = corfuStore.txn(namespace);
        assertThat(tx.getRecord(tableNameA, key1).getPayload()).isEqualTo(key1);
        assertThat(tx.getRecord(tableNameB, key2).getPayload()).isEqualTo(key2);
        assertThat(tx.getRecord(tableNameB, key3).getPayload()).isEqualTo(key3);
        tx.commit();
        runtime.shutdown();
        runtime2.shutdown();
        shutdownCorfuServer(corfuServer);
    }

    private void unregisterTable(CorfuRuntime runtime, String namespace, String tableName) {
        runtime.getObjectsView().TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();

        TableName tableNameKey = TableName.newBuilder()
                .setNamespace(namespace)
                .setTableName(tableName)
                .build();
        runtime.getTableRegistry().getRegistryTable().delete(tableNameKey);

        runtime.getObjectsView().TXEnd();
    }
}
