package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
import com.google.common.math.Quantiles;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.ConsistencyOptions;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.util.serializer.ISerializer;
import org.junit.jupiter.api.Assertions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.apache.commons.lang3.function.Failable;

import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.in;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@Slf4j
public class PersistedCorfuTableTest extends AbstractViewTest implements AutoCloseable {

    private static final String defaultTableName = "diskBackedTable";
    private static final String alternateTableName = "diskBackedTable2";
    private static final String diskBackedDirectory = "/tmp/";

    private static final Path persistedCacheLocation = Paths.get(diskBackedDirectory, alternateTableName);
    private static final int SAMPLE_SIZE = 100;
    private static final int NUM_OF_TRIES = 1;
    private static final int STRING_MIN = 5;
    private static final int STRING_MAX = 10;

    private static final String nonExistingKey = "nonExistingKey";
    private static final String defaultNewMapEntry = "newEntry";
    private static final boolean ENABLE_READ_YOUR_WRITES = true;

    public PersistedCorfuTableTest() {
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
        private final int SERIALIZER_OFFSET = 29;  // Random number.

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

    private PersistedCorfuTable<String, String> setupTable(String streamName, boolean readYourWrites) {
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, streamName);
        final Options options = new Options().setCreateIfMissing(true);
        final ConsistencyOptions consistencyOptions = ConsistencyOptions.builder()
                .readYourWrites(readYourWrites).build();
        return getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<PersistedCorfuTable<String, String>>() {})
                .setArguments(persistedCacheLocation, options, consistencyOptions, new PojoSerializer(String.class), getRuntime())
                .setStreamName(streamName)
                .open();
    }

    private PersistedCorfuTable<String, String> setupTable(boolean readYourWrites) {
        return setupTable(defaultTableName, readYourWrites);
    }

    private PersistedCorfuTable<String, String> setupTable() {
        return setupTable(defaultTableName, ENABLE_READ_YOUR_WRITES);
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
    /*
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
                .setStreamName(defaultTableName)
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
    */


    /**
     * Ensure disk-backed table serialization and deserialization works as expected.
     */
    /*
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
                .setStreamName(defaultTableName)
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
    */

    /**
     * Transactional property based test that does puts followed by scan and filter.
     */
    /*
    @Property(tries = NUM_OF_TRIES)
    void txPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));
            Assertions.assertEquals(intended.size(), table.size());

            executeTx(() -> {
                final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
                Assertions.assertEquals(intended, persisted);
                Assertions.assertEquals(table.size(), persisted.size());
            });
        }
    }
    */


    /**
     * Non-transactional property based test that does puts followed by scan and filter.
     */
    /*
    @Property(tries = NUM_OF_TRIES)
    void nonTxPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(intended.size(), table.size());

            final Set<String> persisted = table.entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
            Assertions.assertEquals(intended, persisted);
            Assertions.assertEquals(table.size(), persisted.size());
        }
    }
    */

    /**
     * Verify basic non-transactional get and insert operations.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxGetAndPut(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            intended.forEach(k -> Assertions.assertEquals(k, table.get(k)));
        }
    }

    ////////////////////////////////////

    /**
     * Test the snapshot isolation property of disk-backed Corfu tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void snapshotIsolation(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);
            final int numUpdates = 5;

            Thread t1 = new Thread(() -> {
                // Initial updates [0, numUpdates).
                for (int i = 0; i < numUpdates; i++) {
                    final int ind = i;
                    executeTx(() -> table.insert(defaultNewMapEntry + ind, defaultNewMapEntry + ind));
                }

                executeTx(() -> {
                    try {
                        log.info("Checking data before thread two performs updates.");
                        for (int i = 0; i < 2*numUpdates; i++) {
                            if (i < numUpdates) {
                                assertThat(table.get(defaultNewMapEntry + i)).isEqualTo(defaultNewMapEntry + i);
                                log.info("key={} has value={}.", defaultNewMapEntry + i, defaultNewMapEntry + i);
                            } else {
                                assertThat(table.get(defaultNewMapEntry + i)).isNull();
                                log.info("key={} has value=null.", defaultNewMapEntry + i);
                            }
                        }

                        log.info("Waiting for thread two.");
                        latch2.countDown();
                        latch1.await();
                        log.info("Waited for thread two. Checking data again.");

                        // Validate that the same snapshot is observed after thread two finishes.
                        for (int i = 0; i < 2*numUpdates; i++) {
                            if (i < numUpdates) {
                                assertThat(table.get(defaultNewMapEntry + i)).isEqualTo(defaultNewMapEntry + i);
                                log.info("key={} has value={}.", defaultNewMapEntry + i, defaultNewMapEntry + i);
                            } else {
                                assertThat(table.get(defaultNewMapEntry + i)).isNull();
                                log.info("key={} has value=null.", defaultNewMapEntry + i);
                            }
                        }

                        log.info("Thread one DONE.");
                    } catch (InterruptedException ignored) {
                        // Ignored
                    }
                });
            });

            Thread t2 = new Thread(() -> {
                try {
                    // Wait until thread one performs initial writes.
                    log.info("Waiting for thread one.");
                    latch2.await();
                    log.info("Waited for thread one. Populating new data.");

                    // Populate additional entries [numUpdates, 2*numUpdates).
                    for (int i = numUpdates; i < 2*numUpdates; i++) {
                        final int ind = i;
                        executeTx(() -> table.insert(defaultNewMapEntry + ind, defaultNewMapEntry + ind));
                    }

                    assertThat(table.get(defaultNewMapEntry + numUpdates)).isEqualTo(defaultNewMapEntry + numUpdates);
                    log.info("Thread two DONE.");
                    latch1.countDown();
                } catch (InterruptedException ex) {
                    // Ignored
                }
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();
        }
    }

    /**
     * Test the read-your-own-writes property of disk-backed Corfu tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void readYourOwnWrites(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {

            executeTx(() -> {
                assertThat(table.get(defaultNewMapEntry)).isNull();
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(table.get(defaultNewMapEntry)).isEqualTo(defaultNewMapEntry);
            });
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void snapshotExpired() {
        resetTests(CorfuRuntimeParameters.builder().mvoCacheExpiry(Duration.ofNanos(0)).build());
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> {
                table.insert("a", "a");
                table.get("a");
            });

            executeTx(() -> {
                assertThat(table.get("a")).isEqualTo("a");

                Thread t1 = new Thread(() -> {
                    executeTx(() -> table.insert("a", "b"));
                    executeTx(() -> table.get("a"));
                });

                t1.start();
                Failable.run(t1::join);

                ((MVOCorfuCompileProxy) table.getCorfuSMRProxy()).getUnderlyingMVO()
                        .getMvoCache().getObjectCache().cleanUp();

                assertThatThrownBy(() -> table.get("a"))
                        .isInstanceOf(TransactionAbortedException.class);
            });

            executeTx(() -> {
                assertThat(table.entryStream().count()).isEqualTo(1);

                Thread t1 = new Thread(() -> {
                    executeTx(() -> table.insert("a", "b"));
                    executeTx(() -> table.get("a"));
                });

                t1.start();
                Failable.run(t1::join);

                ((MVOCorfuCompileProxy) table.getCorfuSMRProxy()).getUnderlyingMVO()
                        .getMvoCache().getObjectCache().cleanUp();

                assertThatThrownBy(() -> table.entryStream().count())
                        .isInstanceOf(TransactionAbortedException.class);
            });

            executeTx(() -> {
                table.insert("a", "a");
                table.insert("b", "b");
                table.insert("c", "c");
                table.get("a");
            });

            final int tableSize = 3;
            executeTx(() -> {
                assertThat(table.entryStream().count()).isEqualTo(tableSize);

                Stream<Map.Entry<String, String>> stream = table.entryStream();
                Iterator<Map.Entry<String, String>> iterator = stream.iterator();
                assertThat(iterator.next()).isNotNull();

                assertThat(iterator.next()).isNotNull();
                // has next failed?

                Thread t1 = new Thread(() -> {
                    executeTx(() -> table.insert("a", "b"));
                    executeTx(() -> table.get("a"));
                });

                t1.start();
                Failable.run(t1::join);

                ((MVOCorfuCompileProxy) table.getCorfuSMRProxy()).getUnderlyingMVO()
                        .getMvoCache().getObjectCache().cleanUp();
                assertThatThrownBy(iterator::next)
                        .isInstanceOf(TrimmedException.class);
            });

        }
    }

    @Property(tries = NUM_OF_TRIES)
    void noReadYourOwnWrites(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(!ENABLE_READ_YOUR_WRITES)) {
            executeTx(() -> {
                assertThat(table.get(defaultNewMapEntry)).isNull();
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(table.get(defaultNewMapEntry)).isEqualTo(null);
            });

        }
    }
        ////////////////////////////////////

    /**
     * Verify RocksDB persisted cache is cleaned up
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyPersistedCacheCleanUp() {
        resetTests();
        try (final PersistedCorfuTable<String, String> table1 = setupTable(alternateTableName, ENABLE_READ_YOUR_WRITES)) {
            table1.insert(defaultNewMapEntry, defaultNewMapEntry);
            assertThat(persistedCacheLocation).exists();
            table1.close();
            assertThat(persistedCacheLocation).doesNotExist();
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void testClear(@ForAll @Size(SAMPLE_SIZE) Set<@AlphaChars String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(alternateTableName, ENABLE_READ_YOUR_WRITES)) {
            executeTx(() -> intended.forEach(entry -> table.insert(entry, entry)));
            executeTx(() -> assertThat(table.entryStream().count()).isEqualTo(intended.size()));
            assertThat(table.entryStream().count()).isEqualTo(intended.size());

            executeTx(table::clear);

            executeTx(() -> assertThat(table.entryStream().count()).isEqualTo(0));
            assertThat(table.entryStream().count()).isEqualTo(0);

            executeTx(() -> intended.forEach(entry -> table.insert(entry, entry)));
            executeTx(() -> assertThat(table.entryStream().count()).isEqualTo(intended.size()));
            assertThat(table.entryStream().count()).isEqualTo(intended.size());

            executeTx(() -> {
                assertThat(table.entryStream().count()).isEqualTo(intended.size());
                intended.forEach(key -> assertThat(table.get(key)).isEqualTo(key));
                table.clear();
                assertThat(table.entryStream().count()).isEqualTo(0);
                intended.forEach(key -> assertThat(table.get(key)).isNull());
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
     * Check {@link PersistedCorfuTable} integration with {@link CorfuStore}.
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
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, defaultTableName);
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
