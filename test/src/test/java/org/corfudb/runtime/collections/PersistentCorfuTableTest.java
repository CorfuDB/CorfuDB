package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ActivitySchedule;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Adult;
import org.corfudb.runtime.ExampleSchemas.Uuid;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.TestSchema;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class PersistentCorfuTableTest extends AbstractViewTest {

    PersistentCorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable;
    CorfuRuntime rt;

    private static final long SMALL_CACHE_SIZE = 3;
    private static final long MEDIUM_CACHE_SIZE = 100;
    private static final long LARGE_CACHE_SIZE = 50_000;

    private static final String someNamespace = "some-namespace";
    private static final String someTable = "some-table";

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     */
    private String getTypeUrl(Descriptors.Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    /**
     * Fully qualified table name created to produce the stream uuid.
     */
    private String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     */
    private <T extends Message> void addTypeToClassMap(@Nonnull final CorfuRuntime runtime, T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        ((ProtobufSerializer)runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE))
                .getClassMap().put(typeUrl, msg.getClass());
    }

    /**
     * Register a giver serializer with a given runtime.
     */
    private void setupSerializer(@Nonnull final CorfuRuntime runtime, @Nonnull final ISerializer serializer) {
        runtime.getSerializers().registerSerializer(serializer);
    }

    /**
     * Register a Protobuf serializer with the default runtime.
     */
    private void setupSerializer() {
        setupSerializer(rt, new ProtobufSerializer(new ConcurrentHashMap<>()));
    }

    private void openTable() {
        corfuTable = openTable(
                rt,
                someNamespace,
                someTable,
                TestSchema.Uuid.class,
                TestSchema.Uuid.class,
                TestSchema.Uuid.class,
                null
        );
    }

    @SneakyThrows
    private <K extends Message, V extends Message, M extends Message>
    PersistentCorfuTable<K, CorfuRecord<V, M>> openTable(@Nonnull final CorfuRuntime runtime,
                                                         @Nonnull final String namespace,
                                                         @Nonnull final String tableName,
                                                         @Nonnull final Class<K> kClass,
                                                         @Nonnull final Class<V> vClass,
                                                         @Nullable final Class<M> mClass,
                                                         @Nullable final CorfuOptions.SchemaOptions schemaOptions) {

        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(runtime, defaultValueMessage);

        if (mClass != null) {
            M defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);
            addTypeToClassMap(runtime, defaultMetadataMessage);
        }

        final String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        PersistentCorfuTable<K, CorfuRecord<V, M>> table = new PersistentCorfuTable$CORFUSMR<>();

        Object[] args = {};

        // If no schema options are provided, omit secondary indexes.
        if (schemaOptions != null) {
            args = new Object[]{new ProtobufIndexer(defaultValueMessage, schemaOptions)};
        }

        table.setCorfuSMRProxy(new MVOCorfuCompileProxy(
                runtime,
                UUID.nameUUIDFromBytes(fullyQualifiedTableName.getBytes()),
                PersistentCorfuTable.<K, CorfuRecord<V, M>>getTableType().getRawType(),
                args,
                runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE),
                new HashSet<UUID>(),
                table
                ));

        return table;
    }

    @Test
    public void testTxn() {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        TestSchema.Uuid key1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid metadata1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord value1 = new CorfuRecord(payload1, metadata1);

        rt.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.put(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        TestSchema.Uuid key2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid payload2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid metadata2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        CorfuRecord value2 = new CorfuRecord(payload2, metadata2);

        TestSchema.Uuid nonExistingKey = TestSchema.Uuid.newBuilder().setLsb(3).setMsb(3).build();

        rt.getObjectsView().TXBegin();

        // Put key2
        corfuTable.put(key2, value2);

        // Table should contain both key1 and key2, but not nonExistingKey
        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 0
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.get(key2)).isNull();
        assertThat(corfuTable.size()).isEqualTo(1);
        rt.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 1
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 1))
                .build()
                .begin();

        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.size()).isEqualTo(2);
        rt.getObjectsView().TXEnd();
    }

    /**
     * Do not allow accessing a stale version which is older than the smallest
     * version of the same object in MVOCache
     */
    @Test
    public void testAccessStaleVersion() {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        // Create 4 versions of the table
        for (int i = 0; i < 4; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }

        // Populate the MVOCache with v1, v2 and v3.
        // v0 is evicted when v3 is put into the cache
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 3))
                .build()
                .begin();

        final TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(0);
        assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(0);
        rt.getObjectsView().TXEnd();

        // Accessing v0 is not allowed
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        Assertions.assertThatExceptionOfType(TransactionAbortedException.class)
                .isThrownBy(() -> corfuTable.get(key).getPayload())
                .withCauseInstanceOf(StaleObjectVersionException.class);

    }

    @Test
    public void simpleParallelAccess() throws InterruptedException {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer();
        openTable();

        int readSize = 100;

        // 1st txn at v0 puts keys {0, .., readSize-1} into the table
        rt.getObjectsView().TXBegin();
        for (int i = 0; i < readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }
        rt.getObjectsView().TXEnd();

        // 2nd txn at v1 puts keys {readSize, ..., readSize*2-1} into the table
        rt.getObjectsView().TXBegin();
        for (int i = readSize; i < 2*readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.put(key, value);
        }
        rt.getObjectsView().TXEnd();

        // Two threads doing snapshot read in parallel
        Thread t1 = new Thread(() -> snapshotRead(0, 0, readSize));
        Thread t2 = new Thread(() -> snapshotRead(1, readSize, 2*readSize));

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private void snapshotRead(long ts, int low, int high) {
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, ts))
                .build()
                .begin();
        for (int i = low; i < high; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(i);
            assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(i);
        }
        rt.getObjectsView().TXEnd();
    }

    // PersistentCorfuTable SecondaryIndexes Tests

    /**
     * Verify that a  lookup by index throws an exception,
     * when the index has never been specified for this CorfuTable.
     */
    @Test (expected = IllegalArgumentException.class)
    public void cannotLookupByIndexWhenIndexNotSpecified() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer();

        PersistentCorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                rt,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                null
        );

        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        for (long i = 0; i < SMALL_CACHE_SIZE; i++) {
            rt.getObjectsView().TXBegin();
            table.insert(Uuid.newBuilder().setLsb(i).setMsb(i).build(),
                    new CorfuRecord<>(ExampleValue.newBuilder()
                            .setPayload("abc")
                            .setAnotherKey(i)
                            .build(), user_1)
            );
            rt.getObjectsView().TXEnd();
        }

        table.getByIndex(() -> "anotherKey", 0);
    }

    /**
     * Verify that a lookup by index on an empty table returns empty.
     */
    @Test
    public void emptyIndexesReturnEmptyValues() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(MEDIUM_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer();

        PersistentCorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                rt,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        rt.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = new ArrayList<>(table.getByIndex(() -> "anotherKey", 0));
        assertThat(entries).isEmpty();

        entries = new ArrayList<>(table.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
        assertThat(entries).isEmpty();
        rt.getObjectsView().TXEnd();
    }

    /**
     * Verify that secondary indexes are updated on removes.
     */
    @Test
    public void doUpdateIndicesOnRemove() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(MEDIUM_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer();

        PersistentCorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                rt,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();
        final long numEntries = 10;

        ArrayList<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>> expectedEntries =
                LongStream.rangeClosed(1, numEntries)
                        .boxed()
                        .map(i -> Pair.of(
                                Uuid.newBuilder().setLsb(i).setMsb(i).build(),
                                new CorfuRecord<>(ExampleValue.newBuilder()
                                        .setPayload("abc")
                                        .setAnotherKey(i)
                                        .setUuid(Uuid.getDefaultInstance())
                                        .build(), user_1)))
                        .collect(Collectors.toCollection(ArrayList::new));

        // Insert entries into table
        for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : expectedEntries) {
            rt.getObjectsView().TXBegin();
            table.insert(entry.getKey(), entry.getValue());
            rt.getObjectsView().TXEnd();
        }

        // Verify secondary indexes
        rt.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = new ArrayList<>(table.getByIndex(() -> "anotherKey", numEntries));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getKey().getLsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getKey().getMsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getValue().getPayload().getAnotherKey()).isEqualTo(numEntries);

        entries = new ArrayList<>(table.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
        assertThat(entries.size()).isEqualTo(numEntries);
        assertThat(entries.containsAll(expectedEntries)).isTrue();
        assertThat(expectedEntries.containsAll(entries)).isTrue();
        rt.getObjectsView().TXEnd();

        // TODO: finish this test
    }

    /**
     * Very basic functionality of secondary indexes.
     */
    @Test
    public void testSecondaryIndexesBasic() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        rt = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(MEDIUM_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer();

        PersistentCorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                rt,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        final UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setLsb(uuid1.getLeastSignificantBits()).setMsb(uuid1.getMostSignificantBits())
                .build();

        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();
        final long eventTime = 123L;
        final UUID randomUUID = UUID.randomUUID();
        Uuid secondaryKey1 = Uuid.newBuilder()
                .setLsb(randomUUID.getLeastSignificantBits()).setMsb(randomUUID.getMostSignificantBits())
                .build();

        rt.getObjectsView().TXBegin();
        table.insert(key1, new CorfuRecord<>(ExampleValue.newBuilder()
                .setPayload("abc")
                .setAnotherKey(eventTime)
                .setUuid(secondaryKey1)
                .build(), user_1));
        rt.getObjectsView().TXEnd();

        rt.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = new ArrayList<>(table.getByIndex(() -> "anotherKey", eventTime));

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
        rt.getObjectsView().TXEnd();

        rt.getObjectsView().TXBegin();
        entries = new ArrayList<>(table.getByIndex(() -> "uuid", secondaryKey1));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
        rt.getObjectsView().TXEnd();
    }
}
