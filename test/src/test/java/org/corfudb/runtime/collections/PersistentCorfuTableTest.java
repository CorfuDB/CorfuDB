package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.Company;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Uuid;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.test.TestSchema;
import org.corfudb.util.TableHelper;
import org.corfudb.util.TableHelper.TableType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.SafeProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.util.TableHelper.openTable;
import static org.corfudb.util.TableHelper.openTablePlain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

@SuppressWarnings("checkstyle:magicnumber")
@RunWith(MockitoJUnitRunner.class)
public class PersistentCorfuTableTest extends AbstractViewTest {
    private static final long SMALL_CACHE_SIZE = 3;
    private static final long MEDIUM_CACHE_SIZE = 100;
    private static final long LARGE_CACHE_SIZE = 50_000;

    private static final String INTERRUPTED_ERROR_MSG = "Unexpected InterruptedException";

    private static final String someNamespace = "some-namespace";
    private static final String someTable = "some-table";

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     */


    /**
     * Fully qualified table name created to produce the stream uuid.
     */
    /**
     * Adds the schema to the class map to enable serialization of this table data.
     */

    /**
     * Register a giver serializer with a given runtime.
     */
    private void setupSerializer(@Nonnull final CorfuRuntime runtime, @Nonnull final ISerializer serializer) {
        runtime.getSerializers().registerSerializer(serializer);
    }

    /**
     * Register a Protobuf serializer with the default runtime.
     */
    private void setupSerializer(CorfuRuntime runtime) {
        setupSerializer(runtime, new ProtobufSerializer(new ConcurrentHashMap<>()));
    }

    //TODO(George): current test needs human observation to verify the optimization
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testMVOGetVersionedObjectOptimization(TableType tableType) throws InterruptedException {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);
        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable =
                openTable(tableType, runtime, someNamespace, someTable);

        for (int i = 0; i < 100; i++) {
            TestSchema.Uuid uuidMsg = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value1 = new CorfuRecord(uuidMsg, uuidMsg);
            corfuTable.insert(uuidMsg, value1);
        }

        AtomicInteger size1 = new AtomicInteger();
        AtomicInteger size2 = new AtomicInteger();
        Thread thread1 = new Thread(() -> {
            runtime.getObjectsView().TXBuild()
                    .type(TransactionType.SNAPSHOT)
                    .snapshot(new Token(0, 9))
                    .build()
                    .begin();
            size1.set(corfuTable.keySet().size());
            runtime.getObjectsView().TXEnd();
        });

        Thread thread2 = new Thread(() -> {
            runtime.getObjectsView().TXBuild()
                    .type(TransactionType.SNAPSHOT)
                    .snapshot(new Token(0, 99))
                    .build()
                    .begin();
            size2.set(corfuTable.keySet().size());
            runtime.getObjectsView().TXEnd();
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(size1.get()).isEqualTo(10);
        assertThat(size2.get()).isEqualTo(100);
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testMultiRuntime(TableType tableType) {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);
        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable =
                openTable(tableType, runtime, someNamespace, someTable);

        TestSchema.Uuid key1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid metadata1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord value1 = new CorfuRecord(payload1, metadata1);

        runtime.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.insert(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        runtime.getObjectsView().TXEnd();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);

        CorfuRuntime rt2 = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(rt2, new ProtobufSerializer(new ConcurrentHashMap<>()));

        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> ct =
                openTable(tableType, rt2, someNamespace, someTable, TestSchema.Uuid.class, TestSchema.Uuid.class, TestSchema.Uuid.class, null);

        assertThat(ct.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(ct.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(ct.size()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testTxn(TableType tableType) {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);
        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable =
                openTable(tableType, runtime, someNamespace, someTable);

        TestSchema.Uuid key1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid metadata1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord value1 = new CorfuRecord(payload1, metadata1);

        runtime.getObjectsView().TXBegin();

        // Table should be empty
        assertThat(corfuTable.get(key1)).isNull();
        assertThat(corfuTable.size()).isZero();

        // Put key1
        corfuTable.insert(key1, value1);

        // Table should now have size 1 and contain key1
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.size()).isEqualTo(1);
        runtime.getObjectsView().TXEnd();

        TestSchema.Uuid key2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid payload2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid metadata2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        CorfuRecord value2 = new CorfuRecord(payload2, metadata2);

        TestSchema.Uuid nonExistingKey = TestSchema.Uuid.newBuilder().setLsb(3).setMsb(3).build();

        runtime.getObjectsView().TXBegin();

        // Put key2
        corfuTable.insert(key2, value2);

        // Table should contain both key1 and key2, but not nonExistingKey
        assertThat(corfuTable.get(key2).getPayload().getLsb()).isEqualTo(payload2.getLsb());
        assertThat(corfuTable.get(key2).getPayload().getMsb()).isEqualTo(payload2.getMsb());
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        runtime.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 0
        runtime.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 0))
                .build()
                .begin();

        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
        assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
        assertThat(corfuTable.get(nonExistingKey)).isNull();
        assertThat(corfuTable.get(key2)).isNull();
        assertThat(corfuTable.size()).isEqualTo(1);
        runtime.getObjectsView().TXEnd();

        // Verify the state of the table @ SEQ 1
        runtime.getObjectsView().TXBuild()
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
        runtime.getObjectsView().TXEnd();

        // Verify the MVOCache has exactly 2 versions
        Set<VersionedObjectIdentifier> voIds = runtime.getObjectsView().getMvoCache().keySet();
        assertThat(voIds).containsExactlyInAnyOrder(
                new VersionedObjectIdentifier(((ICorfuSMR<?>) corfuTable).getCorfuSMRProxy().getStreamID(), -1L),
                new VersionedObjectIdentifier(((ICorfuSMR<?>) corfuTable).getCorfuSMRProxy().getStreamID(), 0L));
    }

    @ParameterizedTest
    @EnumSource(TableType.class)
    public void simpleParallelAccess(TableType tableType) throws InterruptedException {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);
        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable =
                openTable(tableType, runtime, someNamespace, someTable);

        int readSize = 100;

        // 1st txn at v0 puts keys {0, .., readSize-1} into the table
        runtime.getObjectsView().TXBegin();
        for (int i = 0; i < readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.insert(key, value);
        }
        runtime.getObjectsView().TXEnd();

        // 2nd txn at v1 puts keys {readSize, ..., readSize*2-1} into the table
        runtime.getObjectsView().TXBegin();
        for (int i = readSize; i < 2*readSize; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid payload = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            TestSchema.Uuid metadata = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            CorfuRecord value = new CorfuRecord(payload, metadata);
            corfuTable.insert(key, value);
        }
        runtime.getObjectsView().TXEnd();

        // Two threads doing snapshot read in parallel
        Thread t1 = new Thread(() -> snapshotRead(runtime, corfuTable,0, 0, readSize));
        Thread t2 = new Thread(() -> snapshotRead(runtime, corfuTable, 1, readSize, 2*readSize));

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    private void snapshotRead(
            CorfuRuntime runtime,
            ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable,
            long ts, int low, int high) {
        runtime.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, ts))
                .build()
                .begin();
        for (int i = low; i < high; i++) {
            TestSchema.Uuid key = TestSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(i);
            assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(i);
        }
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Verify that a transaction does not observe uncommitted changes by another
     * parallel transaction.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testUncommittedChangesIsolationBetweenParallelTxns(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);
        ICorfuTable<TestSchema.Uuid, CorfuRecord<TestSchema.Uuid, TestSchema.Uuid>> corfuTable = openTable(tableType,
                runtime, someNamespace, someTable);

        TestSchema.Uuid key1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        TestSchema.Uuid payload2 = TestSchema.Uuid.newBuilder().setLsb(2).setMsb(2).build();
        TestSchema.Uuid metadata1 = TestSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        CorfuRecord<TestSchema.Uuid, TestSchema.Uuid> value1 = new CorfuRecord<>(payload1, metadata1);

        // put(k1, v1)
        runtime.getObjectsView().TXBegin();
        corfuTable.insert(key1, value1);
        runtime.getObjectsView().TXEnd();
        assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());

        CountDownLatch readLatch = new CountDownLatch(1);
        CountDownLatch writeLatch = new CountDownLatch(1);
        AtomicLong readerResult = new AtomicLong();
        AtomicLong writerResult = new AtomicLong();

        Thread readerThread = new Thread(() -> {
            runtime.getObjectsView().TXBegin();
            try {
                // Unblocked until writerThread puts uncommitted changes
                readLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Changes made by writeThread should be isolated.
            readerResult.set(corfuTable.get(key1).getPayload().getLsb());

            // Read is done. Signal the writerThread to commit
            writeLatch.countDown();
            runtime.getObjectsView().TXEnd();
        });

        // put(k1, v2) to overwrite the previous put, but do not commit
        Thread writerThread = new Thread(() -> {
            runtime.getObjectsView().TXBegin();
            CorfuRecord<TestSchema.Uuid, TestSchema.Uuid> value2 = new CorfuRecord<>(payload2, metadata1);
            corfuTable.insert(key1, value2);
            writerResult.set(corfuTable.get(key1).getPayload().getLsb());

            // Signals the readerThread to read
            readLatch.countDown();

            try {
                // Unblocked until the readThread has read the table.
                // Without this, the readThread might read this change as a committed transaction.
                writeLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            runtime.getObjectsView().TXEnd();
        });

        readerThread.start();
        writerThread.start();
        readerThread.join();
        writerThread.join();

        assertThat(readerResult.get()).isEqualTo(payload1.getLsb());
        assertThat(writerResult.get()).isEqualTo(payload2.getLsb());
    }

    /**
     * For MVO instances, the ObjectOpenOption.NO_CACHE should ensure that the instance
     * is not saved in ObjectsView.objectCache or MVOCache.objectCache
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNoCacheOption(TableType tableType) {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        setupSerializer(runtime);

        UUID streamA = UUID.randomUUID();
        UUID streamB = UUID.randomUUID();

        PersistentCorfuTable<String, String> tableA = runtime.getObjectsView()
                .build()
                .setStreamID(streamA)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .addOpenOption(ObjectOpenOption.CACHE)
                .open();

        PersistentCorfuTable<String, String> tableB = runtime.getObjectsView()
                .build()
                .setStreamID(streamB)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .addOpenOption(ObjectOpenOption.NO_CACHE)
                .open();

        String key = "key";
        String value = "value";

        tableA.insert(key, value);
        tableB.insert(key, value);

        // Access the table and populate the cache
        tableA.size();
        tableB.size();

        assertThat(runtime.getObjectsView().getObjectCache()).containsOnlyKeys(
                new ObjectsView.ObjectID(streamA, PersistentCorfuTable.class));

        Set<VersionedObjectIdentifier> allKeys = runtime.getObjectsView().getMvoCache().keySet();
        Set<UUID> allObjectIds = allKeys.stream().map(VersionedObjectIdentifier::getObjectId).collect(Collectors.toSet());
        assertThat(allObjectIds).containsOnly(streamA);
    }

    // PersistentCorfuTable SecondaryIndexes Tests - Adapted From CorfuTableTest & CorfuStoreSecondaryIndexTest

    /**
     * Verify that a lookup by index throws an exception,
     * when the index has never been specified for this PersistentCorfuTable.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void cannotLookupByIndexWhenIndexNotSpecified(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                null
        );

        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        for (long i = 0; i < SMALL_CACHE_SIZE; i++) {
            runtime.getObjectsView().TXBegin();
            table.insert(Uuid.newBuilder().setLsb(i).setMsb(i).build(),
                    new CorfuRecord<>(ExampleValue.newBuilder()
                            .setPayload("abc")
                            .setAnotherKey(i)
                            .build(), user_1)
            );
            runtime.getObjectsView().TXEnd();
        }

        if (tableType == TableType.PERSISTED_TABLE) {
            assertThat(table.getByIndex(() -> "anotherKey", 0)).isNull();
        } else {
            assertThatThrownBy(() -> table.getByIndex(() -> "anotherKey", 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

    }

    private <T> List<T> toList(@Nonnull Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
    }

    /**
     * Verify that a lookup by index on an empty table returns empty.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void emptyIndexesReturnEmptyValues(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(MEDIUM_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "anotherKey", 0));
        assertThat(entries).isEmpty();

        entries = toList(table.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
        assertThat(entries).isEmpty();
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Very basic functionality of secondary indexes.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSecondaryIndexesBasic(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(MEDIUM_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
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

        runtime.getObjectsView().TXBegin();
        table.insert(key1, new CorfuRecord<>(ExampleValue.newBuilder()
                .setPayload("abc")
                .setAnotherKey(eventTime)
                .setUuid(secondaryKey1)
                .build(), user_1));
        runtime.getObjectsView().TXEnd();

        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "anotherKey", eventTime));

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
        runtime.getObjectsView().TXEnd();

        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "uuid", secondaryKey1));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Verify that secondary indexes are updated on removes.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void doUpdateIndicesOnRemove(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();
        final long numEntries = 10;

        ArrayList<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>> initialEntries =
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
        for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : initialEntries) {
            runtime.getObjectsView().TXBegin();
            table.insert(entry.getKey(), entry.getValue());
            runtime.getObjectsView().TXEnd();
        }

        // Verify secondary indexes
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "anotherKey", numEntries));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getKey().getLsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getKey().getMsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getValue().getPayload().getAnotherKey()).isEqualTo(numEntries);

        entries = toList(table.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
        assertThat(entries.size()).isEqualTo(numEntries);
        assertThat(entries.containsAll(initialEntries)).isTrue();
        assertThat(initialEntries.containsAll(entries)).isTrue();
        runtime.getObjectsView().TXEnd();

        // Remove entries whose key LSB (UUID) is odd
        ArrayList<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>> expectedEntries =
                initialEntries.stream().filter(entry -> entry.getKey().getLsb() % 2 == 0).collect(Collectors.toCollection(ArrayList::new));

        for (long i = 0; i < numEntries; i++) {
            if (i % 2 != 0) {
                runtime.getObjectsView().TXBegin();
                table.delete(Uuid.newBuilder().setLsb(i).setMsb(i).build());
                runtime.getObjectsView().TXEnd();
            }
        }

        // Verify secondary indexes
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "anotherKey", numEntries));
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getKey().getLsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getKey().getMsb()).isEqualTo(numEntries);
        assertThat(entries.get(0).getValue().getPayload().getAnotherKey()).isEqualTo(numEntries);

        entries = toList(table.getByIndex(() -> "anotherKey", 1L));
        assertThat(entries).isEmpty();

        entries = toList(table.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
        assertThat(entries.size()).isEqualTo(expectedEntries.size());
        assertThat(entries.containsAll(expectedEntries)).isTrue();
        assertThat(expectedEntries.containsAll(entries)).isTrue();
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Very functionality of nested secondary indexes.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNestedSecondaryIndexes(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> table =openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleValue.class).getSchemaOptions()
        );

        // Create 100 records.
        final int totalRecords = 100;
        final long even = 0L;
        final long odd = 1L;
        List<Long> evenRecordIndexes = new ArrayList<>();
        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for(long i = 0; i < totalRecords; i++) {
            if(i % 2 == 0) {
                evenRecordIndexes.add(i);
            }

            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();

            runtime.getObjectsView().TXBegin();
            table.insert(key, new CorfuRecord<>(
                    ExampleValue.newBuilder()
                            .setPayload("payload_" + i)
                            .setAnotherKey(System.currentTimeMillis())
                            .setEntryIndex(i)
                            .setNonPrimitiveFieldLevel0(ExampleSchemas.NonPrimitiveValue.newBuilder()
                                    .setKey1Level1(i % 2 == 0 ? even : odd)
                                    .setKey2Level1(ExampleSchemas.NonPrimitiveNestedValue.newBuilder()
                                            .setKey1Level2(i < (totalRecords / 2) ? "lower half" : "upper half")
                                            .setLevelNumber(2)
                                            .build()))
                            .build(),
                    user));
            runtime.getObjectsView().TXEnd();
        }

        // Get by secondary index, retrieve from database all even entries.
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>> entries = toList(table
                .getByIndex(() -> "non_primitive_field_level_0.key_1_level_1", even));

        assertThat(entries.size()).isEqualTo(totalRecords / 2);

        for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : entries) {
            assertThat(evenRecordIndexes).contains(entry.getValue().getPayload().getEntryIndex());
            evenRecordIndexes.remove(entry.getValue().getPayload().getEntryIndex());
        }

        assertThat(evenRecordIndexes).isEmpty();
        runtime.getObjectsView().TXEnd();

        // Get by secondary index from second level (nested), retrieve from database 'upper half'.
        runtime.getObjectsView().TXBegin();
        entries = toList(table
                .getByIndex(() -> "non_primitive_field_level_0.key_2_level_1.key_1_level_2", "upper half"));

        assertThat(entries.size()).isEqualTo(totalRecords / 2);
        long sum = 0;

        for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : entries) {
            sum += entry.getValue().getPayload().getEntryIndex();
        }

        // Assert sum of consecutive numbers of "upper half" match the expected value.
        assertThat(sum).isEqualTo(((totalRecords / 2) / 2) * ((totalRecords / 2) + (totalRecords - 1)));
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Verify the case of a nested secondary index on REPEATED fields followed by a REPEATED non-primitive
     * field which is directly the indexed value.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNestedSecondaryIndexesWhenIndexedIsNonPrimitiveAndRepeated(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<Company, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                Company.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(Company.class).getSchemaOptions()
        );

        final int totalCompanies = 100;
        List<ExampleSchemas.Department> departments = createApartments();
        createOffices(runtime, departments, totalCompanies, table);

        // Get by secondary index, retrieve from database all Companies that have Department of type 1.
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<Company, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "office.departments", departments.get(0)));
        assertThat(entries.size()).isEqualTo(totalCompanies / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index, retrieve from database all Companies that have Department of Type 4 (all).
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "office.departments", departments.get(3)));
        assertThat(entries.size()).isEqualTo(totalCompanies);
        runtime.getObjectsView().TXEnd();
    }

    private List<ExampleSchemas.Department> createApartments() {
        // Department 1 for office_A and office_C
        ExampleSchemas.Department dpt_1 = ExampleSchemas.Department.newBuilder()
                .addMembers(ExampleSchemas.Member.newBuilder()
                        .addPhoneNumbers("111-111-1111")
                        .setName("Member_DPT1")
                        .build())
                .build();

        // Department 2 for office_B
        ExampleSchemas.Department dpt_2 = ExampleSchemas.Department.newBuilder()
                .addMembers(ExampleSchemas.Member.newBuilder()
                        .addPhoneNumbers("222-222-2222")
                        .setName("Member_DPT2")
                        .build())
                .build();

        // Department 3 for office_B
        ExampleSchemas.Department dpt_3 = ExampleSchemas.Department.newBuilder()
                .addMembers(ExampleSchemas.Member.newBuilder()
                        .addPhoneNumbers("333-333-3333")
                        .setName("Member_DPT3")
                        .build())
                .build();

        // Department 4 for all offices
        ExampleSchemas.Department dpt_4 = ExampleSchemas.Department.newBuilder()
                .addMembers(ExampleSchemas.Member.newBuilder()
                        .addPhoneNumbers("444-444-4444")
                        .setName("Member_DPT4")
                        .build())
                .build();

        return Arrays.asList(dpt_1, dpt_2, dpt_3, dpt_4);
    }

    private void createOffices(
            CorfuRuntime runtime,
            List<ExampleSchemas.Department> departments, int totalCompanies,
            ICorfuTable<Uuid, CorfuRecord<Company, ManagedMetadata>> table) {
        // Even indexed companies will have Office_A and Office_C
        ExampleSchemas.Office office_A = ExampleSchemas.Office.newBuilder()
                .addDepartments(departments.get(0))
                .addDepartments(departments.get(3))
                .build();

        // Odd indexed companies will have Office_B
        ExampleSchemas.Office office_B = ExampleSchemas.Office.newBuilder()
                .addDepartments(departments.get(1))
                .addDepartments(departments.get(2))
                .addDepartments(departments.get(3))
                .build();

        ExampleSchemas.Office office_C = ExampleSchemas.Office.newBuilder()
                .addDepartments(departments.get(0))
                .addDepartments(departments.get(3))
                .build();

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < totalCompanies; i++) {
            UUID id = UUID.randomUUID();
            Uuid networkId = Uuid.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            runtime.getObjectsView().TXBegin();
            if (i % 2 == 0) {
                table.insert(networkId, new CorfuRecord<>(
                        Company.newBuilder().addOffice(office_A).addOffice(office_C).build(),
                        user
                ));
            } else {
                table.insert(networkId, new CorfuRecord<>(
                        Company.newBuilder().addOffice(office_B).build(),
                        user
                ));
            }
            runtime.getObjectsView().TXEnd();
        }
    }

    /**
     * Verify that nested secondary indexes work on repeated fields when the repeated field is
     * not the root level but a nested level.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNestedSecondaryIndexesNestedRepeatedField(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleSchemas.Person, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleSchemas.Person.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleSchemas.Person.class).getSchemaOptions()
        );

        // Create 10 records.
        final int people = 10;
        final String mobileForEvens = "650-123-4567";
        final String mobileForOdds = "408-987-6543";
        final String mobileCommonBoth = "491-999-1111";

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < people; i++) {
            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();

            runtime.getObjectsView().TXBegin();
            table.insert(key, new CorfuRecord<>(
                    ExampleSchemas.Person.newBuilder()
                            .setName("Name_" + i)
                            .setAge(i)
                            .setPhoneNumber(ExampleSchemas.PhoneNumber.newBuilder()
                                    .setHome(UUID.randomUUID().toString())
                                    .addMobile(i % 2 == 0 ? mobileForEvens : mobileForOdds)
                                    .addMobile(mobileCommonBoth)
                                    .build())
                            .build(),
                    user
            ));
            runtime.getObjectsView().TXEnd();
        }

        // Get by secondary index, retrieve from database all even entries.
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleSchemas.Person, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "phoneNumber.mobile", mobileForEvens));
        assertThat(entries.size()).isEqualTo(people / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index, retrieve from database all entries with common mobile number.
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "phoneNumber.mobile", mobileCommonBoth));
        assertThat(entries.size()).isEqualTo(people);
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Verify that nested secondary indexes work on recursive 'repeated' fields.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNestedSecondaryIndexesRecursiveRepeatedFields(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleSchemas.Office, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleSchemas.Office.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleSchemas.Office.class).getSchemaOptions()
        );

        // Create 6 records.
        final int numOffices = 6;
        // Phone number for even index offices
        final String evenPhoneNumber = "222-222-2222";
        // Phone number for odd index offices
        final String oddPhoneNumber = "333-333-3333";
        // Common phone number for all offices
        final String commonPhoneNumber = "000-000-0000";
        // Common home phone number for all offices
        final String homePhoneNumber = "N/A";

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < numOffices; i++) {
            UUID id = UUID.randomUUID();
            Uuid officeId = Uuid.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            runtime.getObjectsView().TXBegin();
            table.insert(officeId, new CorfuRecord<>(
                    ExampleSchemas.Office.newBuilder()
                            // Department 1 per Office
                            .addDepartments(ExampleSchemas.Department.newBuilder()
                                    // Department 1 - Member 1
                                    .addMembers(ExampleSchemas.Member.newBuilder()
                                            .setName("Office_" + i + "_Dpt.1_Member_1")
                                            .addPhoneNumbers(i % 2 == 0 ? evenPhoneNumber : oddPhoneNumber)
                                            .addPhoneNumbers(homePhoneNumber)
                                            .addPhoneNumbers(commonPhoneNumber)
                                            .build())
                                    // Department 1 - Member 2
                                    .addMembers(ExampleSchemas.Member.newBuilder()
                                            .setName("Office_" + i + "_Dpt.1_Member_2")
                                            .addPhoneNumbers(commonPhoneNumber)
                                            .build())
                                    .build())
                            // Department 2 per Office
                            .addDepartments(ExampleSchemas.Department.newBuilder()
                                    // Department 2 - Member 1
                                    .addMembers(ExampleSchemas.Member.newBuilder()
                                            .setName("Office_" + i + "_Dpt.2_Member_1")
                                            .addPhoneNumbers(commonPhoneNumber)
                                            .build())
                                    .build())
                            .build(),
                    user
            ));
            runtime.getObjectsView().TXEnd();
        }

        // Get by secondary index, retrieve from database all offices which have an evenPhoneNumber.
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleSchemas.Office, ManagedMetadata>>> entries = toList(table
                .getByIndex(() -> "departments.members.phoneNumbers", evenPhoneNumber));

        assertThat(entries.size()).isEqualTo(numOffices / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index, retrieve from database all entries with common mobile number.
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "departments.members.phoneNumbers", commonPhoneNumber));
        assertThat(entries.size()).isEqualTo(numOffices);
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Verify that we can access a secondary index based on a custom alias or the default alias.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testSecondaryIndexAlias(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleSchemas.Adult, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleSchemas.Adult.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleSchemas.Adult.class).getSchemaOptions()
        );

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();
        final int adultCount = 50;
        final long adultBaseAge = 30L;
        final long kidsBaseAge = 4L;

        for (int i = 0; i < adultCount; i++) {
            UUID adultId = UUID.randomUUID();
            Uuid adultKey = Uuid.newBuilder()
                    .setMsb(adultId.getMostSignificantBits()).setLsb(adultId.getLeastSignificantBits())
                    .build();

            runtime.getObjectsView().TXBegin();
            final long adultAge = i % 2 == 0 ? adultBaseAge : adultBaseAge * 2;
            final long kidsAge = i % 2 == 0 ? kidsBaseAge : kidsBaseAge * 2;

            table.insert(adultKey, new CorfuRecord<>(
                    ExampleSchemas.Adult.newBuilder()
                            .setPerson(ExampleSchemas.Person.newBuilder()
                                    .setName("Name_" + i)
                                    .setAge(adultAge)
                                    .setPhoneNumber(ExampleSchemas.PhoneNumber.newBuilder()
                                            .setHome(UUID.randomUUID().toString())
                                            .build())
                                    .setChildren(ExampleSchemas.Children.newBuilder()
                                            .addChild(ExampleSchemas.Child.newBuilder().setName("Child_" + i).setAge(kidsAge)).build())
                                    .build()).build(),
                    user
            ));
            runtime.getObjectsView().TXEnd();
        }

        // Get by secondary index (default alias), retrieve from database all adults with adultsBaseAge.
        runtime.getObjectsView().TXBegin();
        List<Map.Entry<Uuid, CorfuRecord<ExampleSchemas.Adult, ManagedMetadata>>>
                entries = toList(table.getByIndex(() -> "age", adultBaseAge));
        assertThat(entries.size()).isEqualTo(adultCount / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index (using fully qualified name), retrieve from database all adults with adultsBaseAge.
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "person.age", adultBaseAge));
        assertThat(entries.size()).isEqualTo(adultCount / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age 'kidsBaseAge'.
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "kidsAge", kidsBaseAge));
        assertThat(entries.size()).isEqualTo(adultCount / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index (fully qualified name), retrieve from database all adults with kids on age 'kidsBaseAge'.
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "person.children.child.age", kidsBaseAge));
        assertThat(entries.size()).isEqualTo(adultCount / 2);
        runtime.getObjectsView().TXEnd();

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age '2' (non-existent).
        runtime.getObjectsView().TXBegin();
        entries = toList(table.getByIndex(() -> "kidsAge", 2));
        assertThat(entries.size()).isZero();
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Test indexing of 'NULL' (i.e., unset non-primitive sub-fields) for the following sub-field patterns
     * (from the root):
     *
     * Refer to SportsProfessional proto, in 'example_schemas.proto' for definitions.
     *
     * (1) Repeated field followed by oneOf field (e.g., hobby.sport)
     * (2) Non-repeated field followed by oneOf field (e.g., profession.sport)
     * (3) Repeated field followed by repeated field (e.g., training.exercises)
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testNestedIndexesWithNullValues(TableType tableType) throws Exception {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(SMALL_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        setupSerializer(runtime);

        ICorfuTable<Uuid, CorfuRecord<ExampleSchemas.SportsProfessional, ManagedMetadata>> table = openTable(
                tableType,
                runtime,
                someNamespace,
                someTable,
                Uuid.class,
                ExampleSchemas.SportsProfessional.class,
                ManagedMetadata.class,
                TableOptions.fromProtoSchema(ExampleSchemas.SportsProfessional.class).getSchemaOptions()
        );

        // Define a player and set only (1) oneOf type, then query for the unset field to confirm this
        // is indexed as NULL (i.e., not set)
        ExampleSchemas.SportsProfessional player1 = ExampleSchemas.SportsProfessional.newBuilder()
                .setPerson(ExampleSchemas.Person.newBuilder().setName("Michael Jordan").build())
                // Set Basket as profession (oneOf field) so query for Baseball as profession
                .setProfession(ExampleSchemas.Hobby.newBuilder().setBasket(ExampleSchemas.Basketball.newBuilder().setTeam("Chicago Bulls").build()).build())
                // Set Baseball as hobby (oneOf field) so query for Basket as hobby
                .addHobby(ExampleSchemas.Hobby.newBuilder().setBaseball(ExampleSchemas.Baseball.newBuilder().build()).build())
                // Do not define any sub-field of repeated type (Exercises) and confirmed its indexed as NULL
                .addTraining(ExampleSchemas.TrainingPlan.newBuilder().build())
                .build();

        // Define a player which does not have any indexed sub-field set (therefore, it should be indexed as NULL)
        ExampleSchemas.SportsProfessional playerUndefined = ExampleSchemas.SportsProfessional.newBuilder()
                .setPerson(ExampleSchemas.Person.newBuilder().setName("Undefined").build())
                // Don't set any 'oneOf' sport for profession (sub-field)
                .setProfession(ExampleSchemas.Hobby.newBuilder().build())
                // Don't set any 'oneOf' sport for Hobby (sub-field)
                .addHobby(ExampleSchemas.Hobby.newBuilder().setBaseball(ExampleSchemas.Baseball.newBuilder().build()).build())
                // Do not define any sub-field of repeated type (Exercises) and confirmed its indexed as NULL
                .addTraining(ExampleSchemas.TrainingPlan.newBuilder().build())
                .build();

        // Add players to Table
        UUID id1 = UUID.randomUUID();
        Uuid idPlayer1 = Uuid.newBuilder()
                .setMsb(id1.getMostSignificantBits()).setLsb(id1.getLeastSignificantBits())
                .build();

        UUID id2 = UUID.randomUUID();
        Uuid idPlayerUndefined = Uuid.newBuilder()
                .setMsb(id2.getMostSignificantBits()).setLsb(id2.getLeastSignificantBits())
                .build();

        runtime.getObjectsView().TXBegin();
        table.insert(idPlayer1, new CorfuRecord<>(
                player1,
                ManagedMetadata.newBuilder().setCreateUser("user_UT").build()
        ));

        table.insert(idPlayerUndefined, new CorfuRecord<>(
                playerUndefined,
                ManagedMetadata.newBuilder().setCreateUser("user_UT").build()
        ));
        runtime.getObjectsView().TXEnd();

        // Query secondary indexes
        // (1) Repeated field followed by oneOf field (e.g., hobby.sport)
        runtime.getObjectsView().TXBegin();
        //List<Map.Entry<Uuid, CorfuRecord<ExampleSchemas.SportsProfessional, ManagedMetadata>>> entries = toList(table.getByIndex(() -> "basketAsHobby", null));
        //assertThat(entries.size()).isEqualTo(2);
        runtime.getObjectsView().TXEnd();

        // (2) Non-repeated field followed by oneOf field (e.g., profession.sport)
        runtime.getObjectsView().TXBegin();
        //entries = toList(table.getByIndex(() -> "baseballPlayers", null));
        //assertThat(entries.size()).isEqualTo(2);
        runtime.getObjectsView().TXEnd();

        // (3) Repeated field followed by repeated field (e.g., training.exercises)
        runtime.getObjectsView().TXBegin();
        //entries = toList(table.getByIndex(() -> "exercises", null));
        //assertThat(entries.size()).isEqualTo(2);
        runtime.getObjectsView().TXEnd();
    }

    /**
     * Test that a table without any updates can be served when a concurrent
     * transaction syncs the stream forward before this first transaction has
     * a chance to request a snapshot proxy.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void testTableNoUpdateInterleave(TableType tableType) {
        addSingleServer(SERVERS.PORT_0);

        ICorfuTable<String, String> table1 = openTablePlain("t1", getDefaultRuntime(), tableType);
        ICorfuTable<String, String> table2 = openTablePlain("t2", getDefaultRuntime(), tableType);
        // Perform some writes to a second table in order to move the global tail.
        final int smallNum = 5;
        for (int i = 0; i < smallNum; i++) {
            getDefaultRuntime().getObjectsView().TXBegin();
            table2.insert(Integer.toString(i), Integer.toString(i));
            getDefaultRuntime().getObjectsView().TXEnd();
        }

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);

        Thread t2 = new Thread(() -> {
            // Wait until the main thread has acquired a transaction timestamp.
            try {
                latch2.await();
            } catch (InterruptedException ex) {
                fail(INTERRUPTED_ERROR_MSG, ex);
            }

            // Move the stream tail for this object.
            getDefaultRuntime().getObjectsView().TXBegin();
            table1.insert("foo", "bar");
            getDefaultRuntime().getObjectsView().TXEnd();

            // Perform an access to sync the underlying MVO to the update from above.
            getDefaultRuntime().getObjectsView().TXBegin();
            assertThat(table1.size()).isNotZero();
            getDefaultRuntime().getObjectsView().TXEnd();

            // Notify the main thread so that it can request a snapshot proxy. The main
            // thread should not see the update above, and should not get a TrimmedException.
            latch1.countDown();
        });

        t2.start();

        getDefaultRuntime().getObjectsView().TXBegin();

        // Acquire a transaction timestamp and wait until the second thread has
        // finished moving the object forward in time.
        assertThat(table2.size()).isNotZero();
        latch2.countDown();
        try {
            latch1.await();
        } catch (InterruptedException ex) {
            fail(INTERRUPTED_ERROR_MSG, ex);
        }

        // Validate that we do not see any updates. A TrimmedException should not be thrown either.
        assertThat(table1.size()).isZero();
        getDefaultRuntime().getObjectsView().TXEnd();

        try {
            t2.join();
        } catch (InterruptedException ex) {
            fail(INTERRUPTED_ERROR_MSG, ex);
        }
    }

    /**
     * Validate that the state of the underlying object is reset when an exception occurs
     * during the sync process. Subsequent reads operations should succeed and not see
     * incomplete or stale data.
     */
    @ParameterizedTest
    @EnumSource(TableType.class)
    public void validateObjectAfterExceptionDuringSync(TableType tableType) {
        addSingleServer(SERVERS.PORT_0);
        CorfuRuntime runtime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        ICorfuTable<String, String> table1 = openTablePlain("t1", runtime, tableType);

        // Populate the table with initial entries.
        final int numEntries = 100;
        for (int i = 0; i < numEntries; i++) {
            runtime.getObjectsView().TXBegin();
            table1.insert(Integer.toString(i), Integer.toString(i));
            runtime.getObjectsView().TXEnd();
        }

        runtime.shutdown();

        CorfuRuntime newRuntime = getNewRuntime(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(LARGE_CACHE_SIZE)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        final CorfuRuntime spyRt = spy(newRuntime);
        final AddressSpaceView spyAddressSpaceView = spy(new AddressSpaceView(spyRt));
        final Long triggerAddress = 80L;

        // Mock the AddressSpace behaviour so that an exception can be thrown
        // midway through the table sync process.
        doReturn(spyAddressSpaceView).when(spyRt).getAddressSpaceView();
        doThrow(new UnreachableClusterException("Cluster is unreachable"))
                .when(spyAddressSpaceView)
                .read(eq(triggerAddress), any(), any());

        table1 = spyRt.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("t1")
                .open();

        Exception triggeredException = null;

        // Attempt a read. This will trigger a sync from the fresh runtime and throw
        // the above exception after reading address 80.
        try {
            spyRt.getObjectsView().TXBegin();
            table1.size();
            spyRt.getObjectsView().TXEnd();
        } catch (Exception ex) {
            triggeredException = ex;
        }

        // Validate that the transaction was aborted with the proper root cause.
        assertThat(triggeredException).isNotNull()
                .isInstanceOf(TransactionAbortedException.class)
                .hasRootCauseExactlyInstanceOf(UnreachableClusterException.class);

        // Remove the mocked behaviour and make sure that the next read does not see partial data.
        triggeredException = null;
        reset(spyAddressSpaceView);

        try {
            spyRt.getObjectsView().TXBegin();
            assertThat(table1.size()).isEqualTo(numEntries);

            for (int i = 0; i < numEntries; i++) {
                assertThat(table1.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }

            spyRt.getObjectsView().TXEnd();
        } catch (Exception ex) {
            triggeredException = ex;
            spyRt.getObjectsView().TXAbort();
        }

        // Validate that no exceptions were thrown.
        assertThat(triggeredException).isNull();
        spyRt.shutdown();
    }
}
