package org.corfudb.integration;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.DynamicMessage;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
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
        TxBuilder tx = store.tx("namespace");
        tx.create("table", uuidKey, uuidVal, metadata)
                .update("table", uuidKey, uuidVal, metadata)
                .commit();
        CorfuRecord record = table.get(uuidKey);
        assertThat(record.getPayload()).isEqualTo(uuidVal);
        assertThat(record.getMetadata()).isEqualTo(metadata);

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * This test is divided into 3 phases.
     * Phase 1: Writes data to CorfuStore in a Table using the transaction builder.
     * Phase 2: Using DynamicMessages, we try to read and edit the message. The lsb in the metadata is updated.
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

        store.openTable(
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
        TxBuilder tx = store.tx(namespace);
        tx.create(tableName, uuidKey, uuidVal, metadata)
                .update(tableName, uuidKey, uuidVal, metadata)
                .commit();

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
        runtime.shutdown();

        // PHASE 3
        // Read using protobuf serializer.
        runtime = createRuntime(singleNodeEndpoint);
        final CorfuStore store3 = new CorfuStore(runtime);

        // Attempting to open an unopened table with the short form should throw the IllegalArgumentException
        assertThatThrownBy(() -> store3.openTable(namespace, tableName)).
        isExactlyInstanceOf(IllegalArgumentException.class);

        // Attempting to open a non-existent table should throw NoSuchElementException
        assertThatThrownBy(() -> store3.openTable(namespace, "NonExistingTableName")).
                isExactlyInstanceOf(NoSuchElementException.class);

        store3.openTable(namespace, tableName, Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().build());
        CorfuRecord<Uuid, ManagedResources> record = store3.query(namespace).getRecord(tableName, uuidKey);
        assertThat(record.getMetadata().getCreateTimestamp()).isEqualTo(newMetadataUuid);

        store3.tx(namespace).update(tableName, uuidKey, uuidVal, metadata).commit();

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
        store.openTable(namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().persistentDataPath(Paths.get(tempDiskPath)).build());

        final long aLong = 1L;
        Uuid uuidVal = Uuid.newBuilder().setMsb(aLong).setLsb(aLong).build();
        ManagedResources metadata = ManagedResources.newBuilder()
                .setCreateTimestamp(aLong).build();
        for (int i = numRecords; i > 0; --i) {
            TxBuilder tx = store.tx(namespace);
            Uuid uuidKey = Uuid.newBuilder().setMsb(aLong+i).setLsb(aLong+i).build();
            tx.create(tableName, uuidKey, uuidVal, metadata)
                    .update(tableName, uuidKey, uuidVal, metadata)
                    .commit();
        }
        final int TEN = 10;
        Set<Uuid> keys = store.query(namespace).keySet(tableName, null);
        Iterables.partition(keys, TEN);

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
        Table<Uuid, Uuid, ManagedResources> table = store2.openTable(namespace, tableName,
                Uuid.class, Uuid.class, ManagedResources.class,
                TableOptions.builder().persistentDataPath(Paths.get(
                        com.google.common.io.Files.createTempDir().getAbsolutePath())).build());
        assertThat(table.count()).isEqualTo(numRecords);

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
        corfuStore.tx(nsxManager)
                .create(tableName, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build())
                .commit();

        corfuStore.tx(nsxManager)
                .update(tableName, key, value,
                        ManagedResources.newBuilder().setCreateUser("CreateUser").build())
                .commit();

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

        corfuStore.tx(nsxManager)
                .update(tableName, value, value,
                        ManagedResources.newBuilder().setCreateUser("CreateUser").build())
                .commit();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }
}
