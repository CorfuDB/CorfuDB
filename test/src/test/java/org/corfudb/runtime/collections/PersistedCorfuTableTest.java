package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
import com.google.common.math.Quantiles;
import com.google.gson.Gson;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.constraints.UniqueElements;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.function.Failable;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;
import org.corfudb.runtime.CorfuOptions.SizeComputationModel;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.Address;
import org.corfudb.runtime.ExampleSchemas.Adult;
import org.corfudb.runtime.ExampleSchemas.Child;
import org.corfudb.runtime.ExampleSchemas.Children;
import org.corfudb.runtime.ExampleSchemas.Person;
import org.corfudb.runtime.ExampleSchemas.PhoneNumber;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.RocksDbReadCommittedTx;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.util.serializer.ISerializer;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.Env;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer.initClientMetrics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

@Slf4j
public class PersistedCorfuTableTest extends AbstractViewTest implements AutoCloseable {

    private static final String defaultTableName = "diskBackedTable";
    private static final String alternateTableName = "diskBackedTable2";
    private static final String diskBackedDirectory = "/tmp/";
    private static final Path persistedCacheLocation = Paths.get(diskBackedDirectory, alternateTableName);
    private static final Options defaultOptions = new Options().setCreateIfMissing(true);
    private static final ISerializer defaultSerializer = new PojoSerializer(String.class);
    private static final int SAMPLE_SIZE = 100;
    private static final int SAMPLE_SIZE_TWO = 2;
    private static final int NUM_OF_TRIES = 1;
    private static final int STRING_MIN = 5;
    private static final int STRING_MAX = 10;

    private static final String nonExistingKey = "nonExistingKey";
    private static final String defaultNewMapEntry = "newEntry";
    private static final boolean ENABLE_READ_YOUR_WRITES = true;
    private static final boolean EXACT_SIZE = true;

    @Captor
    private ArgumentCaptor<String> logCaptor;

    public PersistedCorfuTableTest() {
        AbstractViewTest.initEventGroup();
        resetTests();
    }

    @Override
    public void close() {
        super.cleanupBuffers();
        AbstractViewTest.cleanEventGroup();
    }

    private PersistedCorfuTable<String, String> setupTable(
            String streamName, Index.Registry<String, String> registry,
            Options options, ISerializer serializer) {

        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, streamName));

        getDefaultRuntime().getSerializers().registerSerializer(serializer);

        return getDefaultRuntime().getObjectsView().build()
                .setTypeToken(PersistedCorfuTable.<String, String>getTypeToken())
                .setArguments(persistenceOptions.build(), options, serializer, registry)
                .setStreamName(streamName)
                .setSerializer(serializer)
                .open();
    }

    private PersistedCorfuTable<String, String> setupTable(Index.Registry<String, String> registry) {
        return setupTable(defaultTableName, registry, defaultOptions, defaultSerializer);
    }

    private <V> PersistedCorfuTable<String, V> setupTable(
            String streamName, boolean readYourWrites, boolean exact_size,
            Options options, ISerializer serializer) {

        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, streamName));
        if (!readYourWrites) {
            persistenceOptions.consistencyModel(ConsistencyModel.READ_COMMITTED);
        }

        if (exact_size) {
            persistenceOptions.sizeComputationModel(SizeComputationModel.EXACT_SIZE);
        } else {
            persistenceOptions.sizeComputationModel(SizeComputationModel.ESTIMATE_NUM_KEYS);
        }

        getDefaultRuntime().getSerializers().registerSerializer(serializer);

        return getDefaultRuntime().getObjectsView().build()
                .setTypeToken(PersistedCorfuTable.<String, V>getTypeToken())
                .setArguments(persistenceOptions.build(), options, serializer)
                .setStreamName(streamName)
                .setSerializer(serializer)
                .open();
    }

    private PersistedCorfuTable<String, String> setupTable(String streamName, boolean readYourWrites) {
        return setupTable(streamName, readYourWrites, EXACT_SIZE, defaultOptions, defaultSerializer);
    }

    private PersistedCorfuTable<String, String> setupTable(boolean readYourWrites) {
        return setupTable(defaultTableName, readYourWrites);
    }

    private PersistedCorfuTable<String, String> setupTable(
            String streamName, boolean readYourWrites, boolean exactSize) {
        return setupTable(streamName, readYourWrites, exactSize, defaultOptions, defaultSerializer);
    }

    private PersistedCorfuTable<String, String> setupTable() {
        return setupTable(defaultTableName, ENABLE_READ_YOUR_WRITES);
    }

    private PersistedCorfuTable<String, String> setupTable(String tableName) {
        return setupTable(tableName, ENABLE_READ_YOUR_WRITES);
    }

    /**
     * Executed the specified function in a transaction.
     *
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

        try (final PersistedCorfuTable<String, String> table = setupTable(
                defaultTableName, ENABLE_READ_YOUR_WRITES, EXACT_SIZE, options, defaultSerializer)) {

            final long iterationCount = 100000;
            final int entityCharSize = 1000;

            assertThatThrownBy(() ->
                    LongStream.rangeClosed(1, iterationCount).forEach(idx -> {
                        String key = RandomStringUtils.random(entityCharSize, true, true);
                        String value = RandomStringUtils.random(entityCharSize, true, true);
                        table.insert(key, value);
                        String persistedValue = table.get(key);
                        Assertions.assertEquals(value, persistedValue);
                    })).isInstanceOf(UnrecoverableCorfuError.class)
                    .hasCauseInstanceOf(RocksDBException.class);
        }
    }

    /**
     * Ensure disk-backed table serialization and deserialization works as expected.
     */
    @Property(tries = NUM_OF_TRIES)
    void customSerializer() {
        resetTests();

        try (final PersistedCorfuTable<String, Pojo> table = setupTable(defaultTableName, ENABLE_READ_YOUR_WRITES,
                EXACT_SIZE, defaultOptions, new PojoSerializer(Pojo.class))) {

            final long iterationCount = 100;
            final int entityCharSize = 100;

            LongStream.rangeClosed(1, iterationCount).forEach(idx -> {
                String key = RandomStringUtils.random(entityCharSize, true, true);
                Pojo value = Pojo.builder()
                        .payload(RandomStringUtils.random(entityCharSize, true, true))
                        .build();
                table.insert(key, value);
                Pojo persistedValue = table.get(key);
                Assertions.assertEquals(value, persistedValue);
            });
        }
    }

    /**
     * Transactional property based test that does puts followed by scan and filter.
     */
    @Property(tries = NUM_OF_TRIES)
    void txPutScanAndFilter(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));
            Assertions.assertEquals(intended.size(), table.size());

            executeTx(() -> {
                final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
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
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(intended.size(), table.size());

            final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
            Assertions.assertEquals(intended, persisted);
            Assertions.assertEquals(table.size(), persisted.size());
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void nonTxInsertGetRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(table.size(), intended.size());
            intended.forEach(value -> Assertions.assertEquals(table.get(value), value));
            intended.forEach(table::delete);

            final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
            Assertions.assertTrue(persisted.isEmpty());
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void txInsertGetRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));
            Assertions.assertEquals(table.size(), intended.size());
            executeTx(() -> {
                intended.forEach(value -> Assertions.assertEquals(table.get(value), value));
                intended.forEach(table::delete);
            });

            executeTx(() -> {
                final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
                Assertions.assertTrue(persisted.isEmpty());
            });
        }
    }

    /**
     * Verify basic non-transactional get and insert operations.
     */
    @Property(tries = NUM_OF_TRIES)
    void nonTxGetAndInsert(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            intended.forEach(k -> Assertions.assertEquals(k, table.get(k)));
        }
    }

    /**
     * Verify commit address of transactions for disk-backed tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressMultiTableReadCommitted(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table1 = setupTable(!ENABLE_READ_YOUR_WRITES);
             final PersistedCorfuTable<String, String> table2 = setupTable(alternateTableName)) {
            table1.insert(defaultNewMapEntry, defaultNewMapEntry);

            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isZero();

            intended.forEach(value -> table2.insert(value, value));
            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isZero();
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressMultiTableReadYourWrites(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table1 = setupTable(ENABLE_READ_YOUR_WRITES);
             final PersistedCorfuTable<String, String> table2 = setupTable(alternateTableName)) {
            table1.insert(defaultNewMapEntry, defaultNewMapEntry);

            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isZero();

            intended.forEach(value -> table2.insert(value, value));
            assertThat(executeTx(() -> {
                table1.get(nonExistingKey);
                table2.get(nonExistingKey);
            })).isEqualTo(intended.size());
        }
    }

    /**
     * Verify commit address of interleaving transactions on disk-backed tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressInterleavingTxnReadCommitted(
            @ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        final AtomicBoolean testSucceeded = new AtomicBoolean(true);
        try (final PersistedCorfuTable<String, String> table = setupTable(!ENABLE_READ_YOUR_WRITES)) {
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            Thread t1 = new Thread(() -> {
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(executeTx(() -> table.get(nonExistingKey))).isEqualTo(0L);
                long commitAddress = executeTx(() -> {
                    try {
                        table.get(nonExistingKey);
                        latch2.countDown();
                        latch1.await();
                        table.get(nonExistingKey);
                    } catch (InterruptedException ignored) {
                        // Ignored
                    }
                });

                if (commitAddress != intended.size()) {
                    testSucceeded.set(false);
                }
            });

            Thread t2 = new Thread(() -> {
                try {
                    latch2.await();
                    intended.forEach(value -> table.insert(value, value));
                    long commitAddress = executeTx(() -> table.get(nonExistingKey));
                    if (commitAddress != intended.size()) {
                        testSucceeded.set(false);
                    }
                    latch1.countDown();
                } catch (InterruptedException ex) {
                    // Ignored
                }
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();
            assertThat(testSucceeded.get()).isTrue();
            intended.forEach(value -> assertThat(table.get(value)).isEqualTo(value));
        }
    }

    /**
     * Verify commit address of interleaving transactions on disk-backed tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyCommitAddressInterleavingTxnReadYourWrites(
            @ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        final AtomicBoolean testSucceeded = new AtomicBoolean(true);
        try (final PersistedCorfuTable<String, String> table = setupTable(ENABLE_READ_YOUR_WRITES)) {
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);

            Thread t1 = new Thread(() -> {
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(executeTx(() -> table.get(nonExistingKey))).isEqualTo(0L);
                long commitAddress = executeTx(() -> {
                    try {
                        table.get(nonExistingKey);
                        latch2.countDown();
                        latch1.await();
                        table.get(nonExistingKey);
                    } catch (InterruptedException ignored) {
                        // Ignored
                    }
                });

                if (commitAddress != 0) {
                    testSucceeded.set(false);
                }
            });

            Thread t2 = new Thread(() -> {
                try {
                    latch2.await();
                    intended.forEach(value -> table.insert(value, value));
                    long commitAddress = executeTx(() -> table.get(nonExistingKey));
                    if (commitAddress != intended.size()) {
                        testSucceeded.set(false);
                    }
                    latch1.countDown();
                } catch (InterruptedException ex) {
                    // Ignored
                }
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();
            assertThat(testSucceeded.get()).isTrue();
            intended.forEach(value -> assertThat(table.get(value)).isEqualTo(value));
        }
    }

    /**
     * Test the snapshot isolation property of disk-backed Corfu tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void snapshotIsolation(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            AtomicBoolean failure = new AtomicBoolean(false);
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
                        for (int i = 0; i < 2 * numUpdates; i++) {
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
                        for (int i = 0; i < 2 * numUpdates; i++) {
                            if (i < numUpdates) {
                                assertThat(table.get(defaultNewMapEntry + i)).isEqualTo(defaultNewMapEntry + i);
                                log.info("key={} has value={}.", defaultNewMapEntry + i, defaultNewMapEntry + i);
                            } else {
                                assertThat(table.get(defaultNewMapEntry + i)).isNull();
                                log.info("key={} has value=null.", defaultNewMapEntry + i);
                            }
                        }

                        log.info("Thread one DONE.");
                    } catch (Exception ex) {
                        failure.set(true);
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
                    for (int i = numUpdates; i < 2 * numUpdates; i++) {
                        final int ind = i;
                        executeTx(() -> table.insert(defaultNewMapEntry + ind, defaultNewMapEntry + ind));
                    }

                    assertThat(table.get(defaultNewMapEntry + numUpdates)).isEqualTo(defaultNewMapEntry + numUpdates);
                    log.info("Thread two DONE.");
                    latch1.countDown();
                } catch (Exception ex) {
                    failure.set(true);
                }
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();
            assertThat(failure.get()).isFalse();
        }
    }

    /**
     * Test the read-your-own-writes property of disk-backed Corfu tables.
     */
    @Property(tries = NUM_OF_TRIES)
    void readYourOwnWrites() {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> {
                assertThat(table.get(defaultNewMapEntry)).isNull();
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(table.get(defaultNewMapEntry)).isEqualTo(defaultNewMapEntry);
            });
        }
    }

    /**
     * Ensure that JVM does not crash under an invalid RocksDB instance.
     */
    @Property(tries = NUM_OF_TRIES)
    void invalidRocksDbInstanceConsumeRelease() {
        resetTests();
        final PersistedCorfuTable<String, String> table = setupTable();

        table.insert(defaultNewMapEntry, defaultNewMapEntry);
        assertThat(table.get(defaultNewMapEntry)).isEqualTo(defaultNewMapEntry);
        table.close();

        // Invalid RocksDB instance via consume().
        assertThatThrownBy(() -> table.get(defaultNewMapEntry)).isInstanceOf(TrimmedException.class);

        // Invalid RocksDB instance via release().
        table.close();
    }

    /**
     * Ensure that JVM does not crash under an invalid RocksDB instance.
     */
    @Property(tries = NUM_OF_TRIES)
    void invalidRocksDbInstanceExecuteRelease() {
        resetTests();
        final PersistedCorfuTable<String, String> table = setupTable();
        table.insert(defaultNewMapEntry, defaultNewMapEntry);
        assertThat(table.get(defaultNewMapEntry)).isEqualTo(defaultNewMapEntry);
        executeTx(() -> {
            table.get(defaultNewMapEntry);
            table.close();
            // Invalid RocksDB instance via executeInSnapshot().
            assertThatThrownBy(() -> table.get(defaultNewMapEntry))
                    .isInstanceOf(TransactionAbortedException.class)
                    .hasCauseInstanceOf(TrimmedException.class);
        });
        // Invalid RocksDB instance via release().
        table.close();
    }

    /**
     * After encountering TrimmedException while syncing the object,
     * ensure that all snapshots are released.
     */
    @Property(tries = NUM_OF_TRIES)
    void objectReset() {
        resetTests();

        final AtomicBoolean testSucceeded = new AtomicBoolean(false);
        final CountDownLatch secondAccessLatch = new CountDownLatch(1);
        final CountDownLatch firstAccessLatch = new CountDownLatch(1);

        try (final PersistedCorfuTable<String, String> table = setupTable()) {

            Thread thread = new Thread(() -> executeTx(() -> {
                table.size();
                firstAccessLatch.countDown();
                Failable.run(secondAccessLatch::await);
                try {
                    table.size();
                } catch (TransactionAbortedException abortedException) {
                    if (abortedException.getCause().getMessage().contains("Snapshot is not longer active")) {
                        testSucceeded.set(true);
                    }
                }
            }));

            thread.start();
            Failable.run(firstAccessLatch::await);

            executeTx(() -> table.insert(defaultNewMapEntry, defaultNewMapEntry));
            executeTx(() -> table.insert(defaultNewMapEntry, defaultNewMapEntry));

            final Token token = new Token(getDefaultRuntime().getLayoutView().getLayout().getEpoch(), Integer.MAX_VALUE);
            getDefaultRuntime().getSequencerView().trimCache(Integer.MAX_VALUE);
            getDefaultRuntime().getAddressSpaceView().prefixTrim(token);
            getDefaultRuntime().getAddressSpaceView().invalidateServerCaches();
            getDefaultRuntime().getAddressSpaceView().invalidateClientCache();

            assertThatThrownBy(() -> table.get(defaultNewMapEntry)).isInstanceOf(TrimmedException.class);

            secondAccessLatch.countDown();
            Failable.run(thread::join);
            assertThat(testSucceeded.get()).isTrue();
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void invalidView() {
        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, defaultTableName));

        OptimisticTransactionDB rocksDb = Mockito.mock(OptimisticTransactionDB.class);

        try (DiskBackedCorfuTable<String, String> table = new DiskBackedCorfuTable<>(
                persistenceOptions.build(), defaultOptions, defaultSerializer)) {
            DiskBackedCorfuTable<String, String> newView = table.newView(new RocksDbReadCommittedTx(rocksDb));
            assertThat(newView).isNotNull();
            assertThatThrownBy(() -> newView.newView(new RocksDbReadCommittedTx(rocksDb)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void snapshotExpiredCrud(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests(CorfuRuntimeParameters.builder().mvoCacheExpiry(Duration.ofNanos(0)).build());
        try (final PersistedCorfuTable<String, String> table = setupTable()) {
            executeTx(() -> {
                intended.forEach(entry -> table.insert(entry, entry));
                intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(entry));
            });

            executeTx(() -> {
                intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(entry));

                Thread thread = new Thread(() -> {
                    intended.forEach(entry -> table.insert(entry, StringUtils.reverse(entry)));
                    intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(StringUtils.reverse(entry)));
                });

                thread.start();
                Failable.run(thread::join);

                table.getCorfuSMRProxy().getUnderlyingMVO()
                        .getMvoCache().getObjectCache().cleanUp();

                assertThatThrownBy(() -> table.get(intended.stream().findFirst().get()))
                        .isInstanceOf(TransactionAbortedException.class);
            });

        }
    }

    @Property(tries = NUM_OF_TRIES)
    void snapshotExpiredIterator(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests(CorfuRuntimeParameters.builder().mvoCacheExpiry(Duration.ofNanos(0)).build());
        try (final PersistedCorfuTable<String, String> table = setupTable()) {

            executeTx(() -> {
                intended.forEach(entry -> table.insert(entry, entry));
                intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(entry));
            });

            executeTx(() -> {
                assertThat(table.entryStream().count()).isEqualTo(intended.size());

                Stream<Entry<String, String>> stream = table.entryStream();
                Iterator<Entry<String, String>> iterator = stream.iterator();
                assertThat(iterator.next()).isNotNull();
                assertThat(iterator.next()).isNotNull();

                Thread thread = new Thread(() -> {
                    intended.forEach(entry -> table.insert(entry, StringUtils.reverse(entry)));
                    intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(StringUtils.reverse(entry)));
                });

                thread.start();
                Failable.run(thread::join);

                table.getCorfuSMRProxy().getUnderlyingMVO()
                        .getMvoCache().getObjectCache().cleanUp();
                assertThatThrownBy(iterator::next)
                        .isInstanceOf(TrimmedException.class);
            });
        }
    }

    private void entryStreamIterate(PersistedCorfuTable<String, String> table) {
        final long startTime = System.currentTimeMillis();
        final long duration = TimeUnit.SECONDS.toMillis(10);

        while (System.currentTimeMillis() - startTime < duration) {executeTx(() -> table.entryStream().count());
        }
    }

    /**
     * Two threads will perform a concurrent scan and filter for N seconds.
     * This tests ensure that there are no deadlocks on that path.
     * NOTE: If this tests fails due to a timeout, then that means there is a bug.
     *
     * @param intended
     * @throws InterruptedException
     */
    @Property(tries = NUM_OF_TRIES)
    void concurrentEntryStream(@ForAll @Size(SAMPLE_SIZE_TWO) Set<String> intended) throws InterruptedException {
        resetTests(CorfuRuntimeParameters.builder().mvoCacheExpiry(Duration.ofNanos(0)).build());
        try (final PersistedCorfuTable<String, String> table = setupTable()) {

            executeTx(() -> {
                intended.forEach(entry -> table.insert(entry, entry));
                intended.forEach(entry -> assertThat(table.get(entry)).isEqualTo(entry));
            });

            List<Thread> threads = Arrays.asList(
                    new Thread(() -> entryStreamIterate(table), "T1"),
                    new Thread(() -> entryStreamIterate(table), "T2"));

            threads.forEach(Thread::start);
            for (Thread thread : threads) {
                thread.join();
            }
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void noReadYourOwnWrites(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) throws Exception {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(!ENABLE_READ_YOUR_WRITES)) {
            executeTx(() -> {
                assertThat(table.get(defaultNewMapEntry)).isNull();
                table.insert(defaultNewMapEntry, defaultNewMapEntry);
                assertThat(table.get(defaultNewMapEntry)).isNull();
            });
        }
    }

    /**
     * Verify RocksDB persisted cache is cleaned up
     */
    @Property(tries = NUM_OF_TRIES)
    void verifyPersistedCacheCleanUp() {
        resetTests();
        try (final PersistedCorfuTable<String, String> table1 = setupTable(alternateTableName, ENABLE_READ_YOUR_WRITES)) {
            table1.insert(defaultNewMapEntry, defaultNewMapEntry);
            assertThat(persistedCacheLocation).exists();
        }

        assertThat(persistedCacheLocation).doesNotExist();
    }

    @Property(tries = NUM_OF_TRIES)
    void testEstimateSize(@ForAll @UniqueElements @Size(SAMPLE_SIZE)
                   Set<@AlphaChars @StringLength(min = 1) String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table =
                     setupTable(defaultTableName, ENABLE_READ_YOUR_WRITES, !EXACT_SIZE)) {
            executeTx(() -> {
                intended.forEach(entry -> table.insert(entry, entry));
                // estimateSize() only consults SST.
                assertThat(table.size()).isZero();
            });
            // Now that the data is persisted, it should be reflected.
            executeTx(() -> assertThat(table.size()).isEqualTo(intended.size()));

            executeTx(() -> {
                intended.forEach(entry -> table.insert(entry, StringUtils.reverse(entry)));
                assertThat(table.size()).isEqualTo(intended.size());
            });

            // The database has not been compacted yet.
            executeTx(() -> assertThat(table.size()).isEqualTo(intended.size() * 2));
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void testClear(@ForAll @UniqueElements @Size(SAMPLE_SIZE)
                   Set<@AlphaChars @StringLength(min = 1) String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(new StringIndexer())) {
            executeTx(() -> intended.forEach(entry -> table.insert(entry, entry)));
            executeTx(() -> assertThat(table.entryStream().count()).isEqualTo(intended.size()));
            assertThat(table.entryStream().count()).isEqualTo(intended.size());

            Map<Character, Set<String>> groups = intended.stream()
                    .collect(Collectors.groupingBy(s -> s.charAt(0), Collectors.toSet()));

            groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                            table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                    .map(Entry::getValue)
                    .collect(Collectors.toSet()))
                    .isEqualTo(strings));

            executeTx(() -> {
                table.clear();

                // Ensure correctness from read-your-writes perspective.
                assertThat(table.entryStream().count()).isEqualTo(0);
                intended.forEach(key -> assertThat(table.get(key)).isNull());

                groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                                table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                        .map(Entry::getValue)
                        .collect(Collectors.toSet()))
                        .isEmpty());
            });

            // Ensure correctness from global perspective.
            executeTx(() -> assertThat(table.entryStream().count()).isEqualTo(0));
            assertThat(table.entryStream().count()).isEqualTo(0);

            executeTx(() -> groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                            table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                    .map(Entry::getValue)
                    .collect(Collectors.toSet()))
                    .isEmpty()));
            groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                            table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                    .map(Entry::getValue)
                    .collect(Collectors.toSet()))
                    .isEmpty());
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void testUnmapSecondaryIndexesAndAbort(@ForAll @UniqueElements @Size(SAMPLE_SIZE)
                                           Set<@AlphaChars @StringLength(min = 1) String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(new StringIndexer())) {
            // StringIndexer does not work with empty strings (StringIndexOutOfBoundsException)
            executeTx(() -> intended.forEach(value -> table.insert(StringUtils.reverse(value), value)));

            final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
            assertThat(intended).isEqualTo(persisted);

            Map<Character, Set<String>> groups = persisted.stream()
                    .collect(Collectors.groupingBy(s -> s.charAt(0), Collectors.toSet()));

            executeTx(() -> {
                // Transactional getByIndex.
                groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                                table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                        .map(Entry::getValue)
                        .collect(Collectors.toSet()))
                        .isEqualTo(strings));

                intended.forEach(value -> table.delete(StringUtils.reverse(value)));

                groups.forEach((character, strings) -> assertThat(StreamSupport.stream(
                                table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                        .map(Entry::getValue)
                        .collect(Collectors.toSet()))
                        .isEmpty());

                getDefaultRuntime().getObjectsView().TXAbort();
            });

            groups.forEach(((character, strings) -> assertThat(StreamSupport.stream(
                            table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                    .map(Entry::getValue)
                    .collect(Collectors.toSet()))
                    .isEqualTo(strings)));
        }
    }

    @Property(tries = NUM_OF_TRIES)
    void testSecondaryIndexes(@ForAll @UniqueElements @Size(SAMPLE_SIZE)
                              Set<@AlphaChars @StringLength(min = 1) String> intended) {
        resetTests();
        try (final PersistedCorfuTable<String, String> table = setupTable(new StringIndexer())) {
            executeTx(() -> intended.forEach(value -> table.insert(value, value)));

            {
                final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
                assertThat(intended).isEqualTo(persisted);
                Map<Character, Set<String>> groups = persisted.stream()
                        .collect(Collectors.groupingBy(s -> s.charAt(0), Collectors.toSet()));

                // Non-transactional getByIndex.
                groups.forEach(((character, strings) -> assertThat(StreamSupport.stream(
                                table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                        .map(Entry::getValue)
                        .collect(Collectors.toSet()))
                        .isEqualTo(strings)));
            }

            executeTx(() -> {
                final Set<String> persisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());

                {
                    assertThat(intended).isEqualTo(persisted);
                    Map<Character, Set<String>> groups = persisted.stream()
                            .collect(Collectors.groupingBy(s -> s.charAt(0), Collectors.toSet()));

                    // Transactional getByIndex.
                    groups.forEach(((character, strings) -> assertThat(StreamSupport.stream(
                                    table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                            .map(Entry::getValue)
                            .collect(Collectors.toSet()))
                            .isEqualTo(strings)));
                }

                intended.forEach(value -> table.insert(value, StringUtils.reverse(value)));
                intended.forEach(value -> table.insert(StringUtils.reverse(value), StringUtils.reverse(value)));

                {
                    final Set<String> newPersisted = table.entryStream().map(Entry::getValue).collect(Collectors.toSet());
                    Map<Character, Set<String>> groups = newPersisted.stream()
                            .collect(Collectors.groupingBy(s -> s.charAt(0), Collectors.toSet()));

                    groups.forEach(((character, strings) -> assertThat(StreamSupport.stream(
                                    table.getByIndex(StringIndexer.BY_FIRST_LETTER, character).spliterator(), false)
                            .map(Entry::getValue)
                            .collect(Collectors.toSet()))
                            .isEqualTo(strings)));
                }
            });
        }
    }

    /**
     * A custom generator for {@link Uuid}.
     */
    @Provide
    Arbitrary<Uuid> uuid() {
        return Arbitraries.integers().map(
                idx -> Uuid.newBuilder().setMsb(idx).setLsb(idx).build());
    }

    Arbitrary<ExampleSchemas.Uuid> exampleUuid() {
        return Arbitraries.integers().map(
                idx -> ExampleSchemas.Uuid.newBuilder().setMsb(idx).setLsb(idx).build());
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

    Arbitrary<Child> child() {
        Arbitrary<String> names = Arbitraries.strings().withCharRange('a', 'z')
                .ofMinLength(3).ofMaxLength(21);
        Arbitrary<Integer> ages = Arbitraries.integers();
        return Combinators.combine(names, ages)
                .as((name, age) -> Child.newBuilder().setName(name)
                        .setAge(age).build());
    }

    Arbitrary<Children> children() {
        return Combinators.combine(child(), child())
                .as((child0, child1) -> Children .newBuilder()
                        .addChild(child0)
                        .addChild(child1)
                        .build());
    }

    Arbitrary<PhoneNumber> phoneNumber() {
        return Arbitraries.integers().between(1_000_000, 9_000_000)
                .map(number -> PhoneNumber .newBuilder()
                        .addMobile(Integer.toString(number))
                        .build());
    }

    Arbitrary<Person> person() {
        Arbitrary<String> names = Arbitraries.strings().withCharRange('a', 'z')
                .ofMinLength(3).ofMaxLength(21);
        Arbitrary<Integer> ages = Arbitraries.integers().between(0, 130);

        return Combinators.combine(names, ages, children(), phoneNumber())
                .as((name, age, children, phoneNumber) ->
                        Person.newBuilder()
                                .setName(name)
                                .setAge(age)
                                .setPhoneNumber(phoneNumber)
                                .setChildren(children).build());
    }

    Arbitrary<Address> address() {
        Arbitrary<String> cities = Arbitraries.strings().withCharRange('a', 'z')
                .ofMinLength(3).ofMaxLength(21);
        Arbitrary<String> streets = Arbitraries.strings().withCharRange('a', 'z')
                .ofMinLength(3).ofMaxLength(21);
        Arbitrary<String> units = Arbitraries.strings().withCharRange('a', 'z')
                .ofMinLength(3).ofMaxLength(21);

        Arbitrary<Integer> numbers = Arbitraries.integers().between(1, 128);
        return Combinators.combine(exampleUuid(), cities, streets, units, numbers)
                .as((uuid, city, street, unit, number) -> Address.newBuilder()
                        .setUniqueAddressId(uuid)
                        .setCity(city)
                        .setStreet(street)
                        .setUnit(unit)
                        .setNumber(number)
                        .build());
    }

    Arbitrary<Adult> adult() {
        return Combinators.combine(person(), address())
                .as((person, address) -> Adult.newBuilder()
                        .setPerson(person)
                        .addAddresses(address)
                        .build());
    }

    /**
     * A custom generator for a set of {@link EventInfo}.
     */
    @Provide
    Arbitrary<Set<Uuid>> uuidSet() {
        return uuid().set();
    }

    @Provide
    Arbitrary<Set<EventInfo>> eventInfoSet() {
        return eventInfo().set();
    }

    @Provide
    Arbitrary<Set<Adult>> adultSet() {
        return adult().set();
    }

    /**
     * Test different ways of referencing secondary indexes.
     */
    @Property(tries = NUM_OF_TRIES)
    void testSecondaryIndexesNaming(
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String namespace,
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String tableName,
            @ForAll("uuidSet") @Size(SAMPLE_SIZE + 1) @UniqueElements  Set<Uuid> ids,
            @ForAll("adultSet") @Size(SAMPLE_SIZE + 1) @UniqueElements Set<Adult> adults) throws Exception {
        resetTests();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, defaultTableName);
        try (Table<Uuid, Adult, ManagedResources> table =
                     corfuStore.openTable(namespace, tableName,
                             Uuid.class, Adult.class,
                             ManagedResources.class,
                             // TableOptions includes option to choose - Memory/Disk based corfu table.
                             TableOptions.fromProtoSchema(Adult.class).toBuilder()
                                     .persistentDataPath(persistedCacheLocation)
                                     .build())) {

            ManagedResources metadata = ManagedResources.newBuilder()
                    .setCreateUser("MrProto").build();

            try (TxnContext tx = corfuStore.txn(namespace)) {
                assertThat(adults.size()).isEqualTo(ids.size());
                Streams.zip(ids.stream(), adults.stream(), SimpleEntry::new)
                        .forEach(pair -> tx.putRecord(table, pair.getKey(), pair.getValue(), metadata));
                tx.commit();
            }

            try (TxnContext tx = corfuStore.txn(namespace)) {
                final Set<Adult> adultSet = adults.stream()
                        .map(adult -> tx.getByIndex(table, "person.children.child.age",
                                adult.getPerson().getChildren().getChild(0).getAge()))
                        .flatMap(List::stream)
                        .map(CorfuStoreEntry::getPayload)
                        .collect(Collectors.toSet());
                assertThat(adults).isEqualTo(adultSet);
                tx.commit();
            }

            try (TxnContext tx = corfuStore.txn(namespace)) {
                final Set<Adult> adultSet = adults.stream()
                        .map(adult -> tx.getByIndex(table,
                                "person.children.child",
                                adult.getPerson().getChildren().getChild(0)))
                        .flatMap(List::stream)
                        .map(CorfuStoreEntry::getPayload)
                        .collect(Collectors.toSet());
                assertThat(adults).isEqualTo(adultSet);
                tx.commit();
            }

            try (TxnContext tx = corfuStore.txn(namespace)) {
                final Set<Adult> adultSet = adults.stream()
                        .map(adult -> tx.getByIndex(table,
                                "kidsAge",
                                adult.getPerson().getChildren().getChild(0).getAge()))
                        .flatMap(List::stream)
                        .map(CorfuStoreEntry::getPayload)
                        .collect(Collectors.toSet());
                assertThat(adults).isEqualTo(adultSet);
                tx.commit();
            }
        }

    }

    @Property(tries = NUM_OF_TRIES)
    void dataStoreIntegration(
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String namespace,
            @ForAll @StringLength(min = STRING_MIN, max = STRING_MAX) @AlphaChars String tableName,
            @ForAll("uuidSet") @Size(SAMPLE_SIZE + 1) Set<Uuid> ids,
            @ForAll("eventInfoSet") @Size(SAMPLE_SIZE + 1) Set<EventInfo> events) {
        resetTests();

        final Uuid firstId = ids.stream().findFirst().orElseThrow(IllegalStateException::new);
        final EventInfo firstEvent = events.stream().findAny().orElseThrow(IllegalStateException::new);
        assertThat(ids.remove(firstId)).isTrue();
        assertThat(events.remove(firstEvent)).isTrue();

        assertThat(ids.size()).isEqualTo(SAMPLE_SIZE);
        assertThat(events.size()).isEqualTo(SAMPLE_SIZE);

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        final Path persistedCacheLocation = Paths.get(diskBackedDirectory, defaultTableName);
        try (Table<Uuid, EventInfo, ManagedResources> table =
                     corfuStore.openTable(namespace, tableName,
                             Uuid.class, EventInfo.class,
                             ManagedResources.class,
                             // TableOptions includes option to choose - Memory/Disk based corfu table.
                             TableOptions.fromProtoSchema(EventInfo.class).toBuilder()
                                     .persistentDataPath(persistedCacheLocation)
                                     .build())) {

            ManagedResources metadata = ManagedResources.newBuilder()
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
                assertThat(events.size()).isEqualTo(ids.size());

                Streams.zip(ids.stream(), events.stream(), SimpleEntry::new)
                        .forEach(pair -> tx.putRecord(table, pair.getKey(), pair.getValue(), metadata));
                tx.commit();
            }

            final SimpleEntry<Uuid, EventInfo> sample = Streams
                    .zip(ids.stream(), events.stream(), SimpleEntry::new)
                    .findAny().orElseThrow(() -> new InvalidObjectException("Invalid state."));

            try (TxnContext tx = corfuStore.txn(namespace)) {
                assertThat(tx.getRecord(tableName, sample.getKey()).getPayload())
                        .isEqualTo(sample.getValue());

                final Collection<Message> secondaryIndex = tx
                        .getByIndex(tableName, "event_time", sample.getValue().getEventTime())
                        .stream().map(CorfuStoreEntry::getPayload).collect(Collectors.toList());

                assertThat(secondaryIndex).containsExactly(sample.getValue());

                long medianEventTime = (long) Quantiles.median().compute(events.stream()
                        .map(EventInfo::getEventTime)
                        .collect(Collectors.toList()));

                events.add(firstEvent);
                final Set<EventInfo> filteredEvents = events.stream().filter(
                                event -> event.getEventTime() > medianEventTime)
                        .collect(Collectors.toSet());
                final List<CorfuStoreEntry<Uuid, EventInfo, ManagedResources>> queryResult =
                        tx.executeQuery(tableName,
                                entry -> entry.getPayload().getEventTime() > medianEventTime);
                final Set<EventInfo> scannedValues = queryResult.stream()
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
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    @Test
    public void testExternalProvider() throws InterruptedException, IOException {
        final Logger logger = Mockito.mock(Logger.class);
        final List<String> logMessages = new ArrayList<>();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        final String tableFolderName = "metered-table";

        Mockito.doAnswer(invocation -> {
            synchronized (this) {
                String logMessage = invocation.getArgument(0, String.class);
                if (logMessage.startsWith(tableFolderName)) {
                    countDownLatch.countDown();
                }
                logMessages.add(logMessage);
                return null;
            }
        }).when(logger).debug(logCaptor.capture());

        final Duration loggingInterval = Duration.ofMillis(100);
        initClientMetrics(logger, loggingInterval, PersistedCorfuTableTest.class.toString());

        PersistedCorfuTable<String, String> table = setupTable(tableFolderName);
        countDownLatch.await();
        synchronized (this) {
            assertThat(logMessages.stream().filter(log -> log.contains("rocksdb"))
                    .filter(log -> log.startsWith(tableFolderName))
                    .findAny()).isPresent();
        }

        try (MockedStatic<MeterRegistryProvider> myClassMock =
                     Mockito.mockStatic(MeterRegistryProvider.class)) {
            table.close();
            myClassMock.verify(() -> MeterRegistryProvider.unregisterExternalSupplier(any()), times(1));
        }
    }

    /**
     * Single type POJO serializer.
     */
    public static class PojoSerializer implements ISerializer {

        private static final int SERIALIZER_OFFSET = 29;  // Random number.
        private final Gson gson = new Gson();
        private final Class<?> clazz;

        PojoSerializer(Class<?> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte getType() {
            return SERIALIZER_OFFSET;
        }

        @Override
        public Object deserialize(ByteBuf b, CorfuRuntime rt) {
            return gson.fromJson(new String(ByteBufUtil.getBytes(b)), clazz);
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
}
