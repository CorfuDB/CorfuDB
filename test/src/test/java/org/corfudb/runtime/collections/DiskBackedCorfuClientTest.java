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
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
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
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Disk-backed {@link StreamingMap} tests.
 */
public class DiskBackedCorfuClientTest extends AbstractViewTest implements AutoCloseable {

    private static final String defaultMapName = "diskBackedMap";
    private static final String alternateMapName = "diskBackedMap2";
    private static final String diskBackedDirectory = "/tmp/";

    private static final Path persistedCacheLocation = Paths.get(diskBackedDirectory, alternateMapName);
    private static final int SAMPLE_SIZE = 100;
    private static final int NUM_OF_TRIES = 1;
    private static final int STRING_MIN = 5;
    private static final int STRING_MAX = 10;

    private static final String ANOTHER_KEY_INDEX = "anotherKey";
    private static final String nonExistingKey = "nonExistingKey";
    private static final String defaultNewMapEntry = "newEntry";

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

    private CorfuTable<String, String> setupTable(String streamName) {
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, streamName);
        final Options options = new Options().setCreateIfMissing(true);
        final Supplier<StreamingMap> mapSupplier = () -> new PersistedStreamingMap<String, String>(
                persistedCacheLocation, options,
                new PojoSerializer(String.class), getRuntime());
        return getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC)
                .setStreamName(streamName)
                .open();
    }

    private CorfuTable<String, String> setupTable() {
        return setupTable(defaultMapName);
    }

    /**
     * Executed the specified function in a transaction.
     * @param functor function which will be executed within a transaction
     * @return the address of the commit
     */
    private long executeTx(Runnable functor) {
        long commitAddress;
        getDefaultRuntime().getObjectsView().TXBegin();
        try {
            functor.run();
        } finally {
            commitAddress = getDefaultRuntime().getObjectsView().TXEnd();
        }

        return commitAddress;
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
                .setArguments(mapSupplier,ICorfuVersionPolicy.MONOTONIC)
                .setStreamName(defaultMapName)
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
                .setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC)
                .setStreamName(defaultMapName)
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
     * Transactional property based test that does puts followed by scan and filter.
     */
    @Property(tries = NUM_OF_TRIES)
    void txPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.put(value, value)));
            Assertions.assertEquals(intended.size(), table.size());

            executeTx(() -> {
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
                Assertions.assertEquals(intended, persisted);
                Assertions.assertEquals(table.size(), persisted.size());
            });
        }
    }


    /**
     * Non-transactional property based test that does puts followed by scan and filter.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.put(value, value));
            Assertions.assertEquals(intended.size(), table.size());

            final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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

            final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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

            final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
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
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
                Assertions.assertEquals(persisted.size(), 0);
                Assertions.assertEquals(table.size(), 0);
            });
        }
    }

    /**
     * Verify commit address of empty transactions.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressEmpty(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            assertThat(executeTx(() -> {})).isEqualTo(Address.NON_ADDRESS);
            intended.forEach(value -> table.put(value, value));
            assertThat(executeTx(() -> {})).isEqualTo(intended.size() - 1);
        }
    }

    /**
     * Verify commit address of transactions for disk-backed tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressMultiTable(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final CorfuTable<String, String> table1 = setupTable();
             final CorfuTable<String, String> table2 = setupTable(alternateMapName)) {
            table1.put(defaultNewMapEntry, defaultNewMapEntry);

            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isZero();

            intended.forEach(value -> table2.put(value, value));
            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isZero();
        }
    }

    /**
     * Verify commit address of interleaving transactions on disk-backed tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressInterleavingTxn(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final CorfuTable<String, String> table = setupTable()) {
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            Thread t1 = new Thread(() -> {
                table.put(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(executeTx(() -> table.get(nonExistingKey))).isEqualTo(0L);
                assertThat(executeTx(() -> {
                    try {
                        table.get(nonExistingKey);
                        latch2.countDown();
                        latch1.await();
                        table.get(nonExistingKey);
                    } catch (InterruptedException ignored) {
                        // Ignored
                    }
                })).isEqualTo(intended.size());
            });

            Thread t2 = new Thread(() -> {
                try {
                    latch2.await();
                    intended.forEach(value -> table.put(value, value));
                    assertThat(executeTx(() -> table.get(nonExistingKey))).isEqualTo(intended.size());
                    latch1.countDown();
                } catch (InterruptedException ex) {
                    // Ignored
                }
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();
            intended.forEach(value -> assertThat(table.get(value)).isEqualTo(value));
        }
    }

    /**
     * Verify RocksDB persisted cache is cleaned up
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyPersistedCacheCleanUp() {
        resetTests();
        assertThat(persistedCacheLocation).doesNotExist();
        try (final CorfuTable<String, String> table1 = setupTable(alternateMapName)) {
            table1.put(defaultNewMapEntry, defaultNewMapEntry);
            assertThat(persistedCacheLocation).exists();
            table1.close();
            assertThat(persistedCacheLocation).doesNotExist();
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

    @Property(tries = NUM_OF_TRIES)
    void disableSecondaryIndexes(
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String namespace,
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String tableName)
            throws Exception {
        resetTests();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());

        { // Positive test.
            final Table<Uuid, ExampleValue, SampleSchema.ManagedResources> table =
                    corfuStore.openTable(namespace, tableName,
                            Uuid.class, ExampleValue.class,
                            SampleSchema.ManagedResources.class,
                            // TableOptions includes option to choose - Memory/Disk based corfu table.
                            TableOptions.fromProtoSchema(ExampleValue.class).toBuilder()
                                    .persistentDataPath(Paths.get(diskBackedDirectory, tableName))
                                    .secondaryIndexesDisabled(true).build());

            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> table.getByIndex(ANOTHER_KEY_INDEX, 0L))
                    .withMessage("Secondary Index anotherKey is not defined.");
        }
        { // Negative test.
            final Table<Uuid, ExampleValue, SampleSchema.ManagedResources> table =
                    corfuStore.openTable(namespace, tableName + tableName,
                            Uuid.class, ExampleValue.class,
                            SampleSchema.ManagedResources.class,
                            // TableOptions includes option to choose - Memory/Disk based corfu table.
                            TableOptions.fromProtoSchema(ExampleValue.class).toBuilder()
                                    .persistentDataPath(Paths.get(diskBackedDirectory, tableName + tableName))
                                    .build());

            // Negative test. No throw.
            table.getByIndex(ANOTHER_KEY_INDEX, 0L);
        }
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
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, defaultMapName);
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
        table.put(firstId, firstEvent, metadata);

        // Fetch timestamp to perform snapshot queries or transactions at a particular timestamp.
        Token token = getDefaultRuntime().getSequencerView().query().getToken();
        CorfuStoreMetadata.Timestamp timestamp = CorfuStoreMetadata.Timestamp.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();

        try (TxnContext tx = corfuStore.txn(namespace)) {
            Streams.zip(ids.stream(), events.stream(), SimpleEntry::new)
                    .forEach(pair -> tx.putRecord(table, pair.getKey(), pair.getValue(), metadata));
            tx.commit();
        }

        SimpleEntry<Uuid, EventInfo> sample = Streams
                .zip(ids.stream(), events.stream(), SimpleEntry::new)
                .findAny().orElseThrow(() -> new InvalidObjectException("Invalid state."));

        try (TxnContext tx = corfuStore.txn(namespace)) {
            assertThat(tx.getRecord(tableName, sample.getKey()).getPayload())
                    .isEqualTo(sample.getValue());

            Collection<Message> secondaryIndex = tx
                    .getByIndex(tableName, "event_time", sample.getValue().getEventTime())
                    .stream().map(e -> e.getPayload()).collect(Collectors.toList());
            assertThat(secondaryIndex).containsExactly(sample.getValue());

            long medianEventTime = (long) Quantiles.median().compute(events.stream()
                    .map(EventInfo::getEventTime)
                    .collect(Collectors.toList()));

            events.add(firstEvent);
            Set<EventInfo> filteredEvents = events.stream().filter(
                    event -> event.getEventTime() > medianEventTime)
                    .collect(Collectors.toSet());
            List<CorfuStoreEntry<Uuid, EventInfo, SampleSchema.ManagedResources>> queryResult =
                    tx.executeQuery(tableName,
                            record -> ((EventInfo) record.getPayload()).getEventTime() > medianEventTime);
            Set<EventInfo> scannedValues = queryResult.stream()
                    .map(CorfuStoreEntry::getPayload).collect(Collectors.toSet());

            assertThat(filteredEvents.size()).isGreaterThan(0).isLessThan(SAMPLE_SIZE);
            assertThat(scannedValues.size()).isEqualTo(filteredEvents.size());
            assertThat(tx.count(tableName)).isEqualTo(SAMPLE_SIZE + 1);
            tx.commit();
        }

        try (TxnContext tx = corfuStore.txn(namespace, IsolationLevel.snapshot(timestamp))) {
            assertThat(tx.count(tableName)).isEqualTo(1);
            tx.commit();
        }

        assertThat(corfuStore.listTables(namespace))
                .containsExactly(CorfuStoreMetadata.TableName.newBuilder()
                        .setNamespace(namespace)
                        .setTableName(tableName)
                        .build());
    }
}
