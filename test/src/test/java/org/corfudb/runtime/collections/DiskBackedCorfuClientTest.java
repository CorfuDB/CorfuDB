package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
import com.google.common.math.Quantiles;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;

import lombok.Builder;
import lombok.Data;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.jupiter.api.Assertions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;

import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Disk-backed {@link StreamingMap} tests.
 */
public class DiskBackedCorfuClientTest extends AbstractViewTest implements AutoCloseable {

    private final static Path persistedCacheLocation = Paths.get("/tmp/", "diskBackedMap2");
    private final static int SAMPLE_SIZE = 100;
    private final static int NUM_OF_TRIES = 1;
    private final static int STRING_MIN = 5;
    private final static int STRING_MAX = 10;


    public DiskBackedCorfuClientTest() {
        AbstractViewTest.initEventGroup();
        resetTests();
    }

    @Override
    public void close() {
        super.cleanupBuffers();
        AbstractViewTest.cleanEventGroup();
    }

    /**
     * Single type POJO serializer.
     */
    public static class PojoSerializer implements ISerializer {
        private final Gson gson = new Gson();
        private final Class<?> clazz;
        private final int SERIALIZER_OFFSET = 23;  // Random number.

        PojoSerializer(Class<?> clazz) {
            this.clazz = clazz;
        }

        private byte[] byteArrayFromBuf(final ByteBuf buf) {
            ByteBuf readOnlyCopy = buf.asReadOnly();
            readOnlyCopy.resetReaderIndex();
            byte[] outArray = new byte[readOnlyCopy.readableBytes()];
            readOnlyCopy.readBytes(outArray);
            return outArray;
        }

        @Override
        public byte getType() {
            return SERIALIZER_OFFSET;
        }

        @Override
        public Object deserialize(ByteBuf b, CorfuRuntime rt) {
            return gson.fromJson(new String(byteArrayFromBuf(b)), clazz);
        }

        @Override
        public void serialize(Object o, ByteBuf b) {
            b.writeBytes(gson.toJson(o).getBytes());
        }
    }

    /**
     * Sample POJO class.
     */
    @Data
    @Builder
    public static class Pojo {
        public final String payload;
    }

    private CorfuTable<String, String> setupTable() {
        final Path persistedCacheLocation = Paths.get("/tmp/", "diskBackedMap");
        final Options options = new Options().setCreateIfMissing(true);
        final Supplier<StreamingMap> mapSupplier = () -> new PersistedStreamingMap<String, String>(
                persistedCacheLocation, options,
                new PojoSerializer(String.class), getRuntime());
        return getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(mapSupplier, ICorfuVersionPolicy.DEFAULT)
                .setStreamName("diskBackedMap")
                .open();
    }

    /**
     * Executed the specified function in a transaction.
     *
     * @param functor function which will be executed within a transaction
     */
    private void executeTx(Runnable functor) {
        getDefaultRuntime().getObjectsView().TXBegin();
        try {
            functor.run();
        } finally {
            getDefaultRuntime().getObjectsView().TXEnd();
        }

    }

    @Override
    public void resetTests() {
        RocksDB.loadLibrary();
        super.resetTests();
    }

    /**
     * Ensure that file-system quota is obeyed.
     *
     * @throws RocksDBException should not be thrown
     */
    @Property(tries = NUM_OF_TRIES)
    void fileSystemLimit() throws Exception {
        resetTests();

        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());
        sstFileManager.setMaxAllowedSpaceUsage(FileUtils.ONE_KB);
        sstFileManager.setCompactionBufferSize(FileUtils.ONE_KB);

        final Options options =
                new Options().setCreateIfMissing(true)
                        .setSstFileManager(sstFileManager)
                        // The size is checked either during flush or compaction.
                        .setWriteBufferSize(FileUtils.ONE_KB);

        final Supplier<StreamingMap> mapSupplier = () -> new PersistedStreamingMap<String, String>(
                persistedCacheLocation, options, Serializers.JSON, getRuntime());
        final CorfuTable<String, String> table = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(mapSupplier,ICorfuVersionPolicy.DEFAULT)
                .setStreamName("diskBackedMap")
                .open();

        final long ITERATION_COUNT = 100000;
        final int ENTITY_CHAR_SIZE = 1000;

        assertThatThrownBy(() ->
                LongStream.rangeClosed(1, ITERATION_COUNT).forEach(idx -> {
                    String key = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
                    String value = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
                    table.put(key, value);
                    String persistedValue = table.get(key);
                    Assertions.assertEquals(value, persistedValue);
                })).isInstanceOf(UnrecoverableCorfuError.class)
                .hasCauseInstanceOf(RocksDBException.class);

        table.close();
    }


    /**
     * Ensure disk-backed table serialization and deserialization works as expected.
     */
    @Property(tries = NUM_OF_TRIES)
    void customSerializer() {
        resetTests();
        final Options options =
                new Options().setCreateIfMissing(true)
                        // The size is checked either during flush or compaction.
                        .setWriteBufferSize(FileUtils.ONE_KB);
        final Supplier<StreamingMap> mapSupplier = () -> new PersistedStreamingMap<String, Pojo>(
                persistedCacheLocation, options, new PojoSerializer(Pojo.class), getRuntime());
        CorfuTable<String, Pojo>
                table = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, Pojo>>() {})
                .setArguments(mapSupplier, ICorfuVersionPolicy.DEFAULT)
                .setStreamName("diskBackedMap")
                .open();

        final long ITERATION_COUNT = 100;
        final int ENTITY_CHAR_SIZE = 100;

        LongStream.rangeClosed(1, ITERATION_COUNT).forEach(idx -> {
            String key = RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true);
            Pojo value = Pojo.builder()
                    .payload(RandomStringUtils.random(ENTITY_CHAR_SIZE, true, true))
                    .build();
            table.put(key, value);
            Pojo persistedValue = table.get(key);
            Assertions.assertEquals(value, persistedValue);
        });

        table.close();
    }

    /**
     * Non-transactional property based test that does puts followed by scan and filter.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.put(value, value)));
            Assertions.assertEquals(intended.size(), table.size());

            executeTx(() -> {
                final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
                Assertions.assertEquals(intended, persisted);
                Assertions.assertEquals(table.size(), persisted.size());
            });
        }
    }


    /**
     * Transactional property based test that does puts followed by scan and filter.
     */
    @Property(tries = NUM_OF_TRIES)
    void txPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.put(value, value));
            Assertions.assertEquals(intended.size(), table.size());

            final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
            Assertions.assertEquals(intended, persisted);
            Assertions.assertEquals(table.size(), persisted.size());
        }
    }

    /**
     * Non-transactional property based test that does puts followed by gets and removes.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxPutGetRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.put(value, value));
            Assertions.assertEquals(table.size(), intended.size());
            intended.forEach(value -> Assertions.assertEquals(table.get(value), value));
            intended.forEach(value -> Assertions.assertEquals(table.remove(value), value));

            final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
            Assertions.assertTrue(persisted.isEmpty());
        }
    }

    /**
     * Transactional property based test that does puts followed by gets and removes.
     */
    @Property(tries = NUM_OF_TRIES)
    void txPutGetRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.put(value, value)));
            Assertions.assertEquals(table.size(), intended.size());
            executeTx(() -> {
                intended.forEach(value -> Assertions.assertEquals(table.get(value), value));
                intended.forEach(table::remove);
            });

            executeTx(() -> {
                final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
                Assertions.assertTrue(persisted.isEmpty());
            });
        }
    }

    /**
     * Transactional property based test that does puts followed by gets and removes.
     */
    @Property(tries = NUM_OF_TRIES)
    void mapKeySet(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.put(value, value)));
            Assertions.assertEquals(table.keySet(), intended);
        }
    }

    /**
     * Non-transactional property based test that does inserts followed by removes.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxInsertRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));
            Assertions.assertEquals(table.size(), intended.size());
            executeTx(() ->  intended.forEach(table::delete));

            executeTx(() -> {
                final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
                Assertions.assertTrue(persisted.isEmpty());
            });
        }
    }

    /**
     * Transactional property based test that does inserts followed by removes.
     */
    @Property(tries = NUM_OF_TRIES)
    void txInsertRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(table.size(), intended.size());
            intended.forEach(table::delete);

            executeTx(() -> {
                final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
                Assertions.assertTrue(persisted.isEmpty());
            });
        }
    }

    /**
     * Non-transactional property based test that does inserts followed by clear.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxInsertClear(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(table.size(), intended.size());
            table.clear();

            final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
            Assertions.assertEquals(persisted.size(), 0);
            Assertions.assertEquals(table.size(), 0);
        }
    }

    /**
     * Transactional property based test that does inserts followed by clear.
     */
    @Property(tries = NUM_OF_TRIES)
    void txInsertClear(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));
            Assertions.assertEquals(table.size(), intended.size());
            executeTx(table::clear);

            executeTx(() -> {
                final Set<String> persisted = new HashSet<>(table.scanAndFilter(entry -> true));
                Assertions.assertEquals(persisted.size(), 0);
                Assertions.assertEquals(table.size(), 0);
            });
        }
    }

    /**
     * A custom generator for a set of {@link Uuid}.
     */
    @Provide
    Arbitrary<Set<Uuid>> uuidSet() {
        return uuid().set();
    }

    /**
     * A custom generator for {@link Uuid}.
     */
    @Provide
    Arbitrary<Uuid> uuid() {
        return Arbitraries.integers().map(idx -> UUID.randomUUID()).map(
                uuid -> Uuid.newBuilder()
                        .setMsb(uuid.getMostSignificantBits())
                        .setLsb(uuid.getLeastSignificantBits())
                        .build());
    }

    /**
     * A custom generator for a set of {@link EventInfo}.
     */
    @Provide
    Arbitrary<Set<EventInfo>> eventInfoSet() {
        return eventInfo().set();
    }

    /**
     * A custom generator for {@link EventInfo}.
     */
    @Provide
    Arbitrary<EventInfo> eventInfo() {
        return Arbitraries.integers().map(idx ->
                EventInfo.newBuilder()
                        .setId(idx)
                        .setName("event_" + idx)
                        .setEventTime(idx)
                        .build());
    }

    /**
     * Check {@link PersistedStreamingMap} integration with {@link CorfuStore}.
     */
    void dataStoreIntegration(
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String namespace,
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String tableName,
            @ForAll("uuid") Uuid firstId,
            @ForAll("eventInfo") EventInfo firstEvent,
            @ForAll("uuidSet") @Size(SAMPLE_SIZE) Set<Uuid> ids,
            @ForAll("eventInfoSet") @Size(SAMPLE_SIZE) Set<EventInfo> events)
            throws Exception {
        resetTests();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        final Path persistedCacheLocation = Paths.get("/tmp/", "diskBackedMap");
        final Table<Uuid, EventInfo, SampleSchema.ManagedResources> table =
                corfuStore.openTable(namespace, tableName,
                        Uuid.class, EventInfo.class,
                        SampleSchema.ManagedResources.class,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().persistentDataPath(persistedCacheLocation).build());

        SampleSchema.ManagedResources metadata = SampleSchema.ManagedResources.newBuilder()
                .setCreateUser("MrProto").build();

        // Simple CRUD using the table instance.
        // These are wrapped as transactional operations.
        table.create(firstId, firstEvent, metadata);

        // Fetch timestamp to perform snapshot queries or transactions at a particular timestamp.
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        TxBuilder tx = corfuStore.tx(namespace);

        Streams.zip(ids.stream(), events.stream(), SimpleEntry::new)
                .forEach(pair -> tx.update(tableName, pair.getKey(), pair.getValue(), metadata));
        tx.commit();

        SimpleEntry<Uuid, EventInfo> sample = Streams
                .zip(ids.stream(), events.stream(), SimpleEntry::new)
                .findAny().orElseThrow(() -> new InvalidObjectException("Invalid state."));

        Query query = corfuStore.query(namespace);
        assertThat(query.getRecord(tableName, sample.getKey()).getPayload())
                .isEqualTo(sample.getValue());

        Collection<Message> secondaryIndex = query
                .getByIndex(tableName, "event_time", sample.getValue().getEventTime())
                .getResult().stream().map(Map.Entry::getValue).collect(Collectors.toList());
        assertThat(secondaryIndex).containsExactly(sample.getValue());

        long medianEventTime = (long) Quantiles.median().compute(events.stream()
                .map(EventInfo::getEventTime)
                .collect(Collectors.toList()));

        events.add(firstEvent);
        Set<EventInfo> filteredEvents = events.stream().filter(
                event -> event.getEventTime() > medianEventTime)
                .collect(Collectors.toSet());
        QueryResult<CorfuStoreEntry<Uuid, EventInfo, SampleSchema.ManagedResources>> queryResult =
                query.executeQuery(tableName,
                        record -> ((EventInfo) record.getPayload()).getEventTime() > medianEventTime);
        Set<EventInfo> scannedValues = queryResult.getResult().stream()
                .map(CorfuStoreEntry::getPayload).collect(Collectors.toSet());

        assertThat(filteredEvents.size()).isGreaterThan(0).isLessThan(SAMPLE_SIZE);
        assertThat(scannedValues.size()).isEqualTo(filteredEvents.size());

        assertThat(query.count(tableName, timestamp)).isEqualTo(1);
        assertThat(query.count(tableName)).isEqualTo(SAMPLE_SIZE + 1);

        assertThat(corfuStore.listTables(namespace))
                .containsExactly(CorfuStoreMetadata.TableName.newBuilder()
                        .setNamespace(namespace)
                        .setTableName(tableName)
                        .build());
    }
}
