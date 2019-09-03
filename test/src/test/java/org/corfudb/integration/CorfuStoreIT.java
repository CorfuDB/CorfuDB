package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.DynamicMessage;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

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
}
