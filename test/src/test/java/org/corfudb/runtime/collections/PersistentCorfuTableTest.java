package org.corfudb.runtime.collections;

import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.Adult;
import org.corfudb.runtime.ExampleSchemas.Baseball;
import org.corfudb.runtime.ExampleSchemas.Basketball;
import org.corfudb.runtime.ExampleSchemas.Child;
import org.corfudb.runtime.ExampleSchemas.Children;
import org.corfudb.runtime.ExampleSchemas.Company;
import org.corfudb.runtime.ExampleSchemas.Department;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.Hobby;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Member;
import org.corfudb.runtime.ExampleSchemas.NonPrimitiveNestedValue;
import org.corfudb.runtime.ExampleSchemas.NonPrimitiveValue;
import org.corfudb.runtime.ExampleSchemas.Office;
import org.corfudb.runtime.ExampleSchemas.Person;
import org.corfudb.runtime.ExampleSchemas.PhoneNumber;
import org.corfudb.runtime.collections.table.GenericCorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.test.CacheSizeForTest;
import org.corfudb.test.TestSchema.Uuid;
import org.corfudb.test.managedtable.ManagedCorfuTable;
import org.corfudb.test.managedtable.ManagedCorfuTable.ManagedCorfuTableConfig;
import org.corfudb.test.managedtable.ManagedCorfuTableSetupManager;
import org.corfudb.test.managedtable.ManagedRuntime;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.runtime.ExampleSchemas.SportsProfessional;
import static org.corfudb.runtime.ExampleSchemas.TrainingPlan;
import static org.corfudb.runtime.view.ObjectsView.ObjectID;
import static org.corfudb.test.RtParamsForTest.getLargeRtParams;
import static org.corfudb.test.RtParamsForTest.getMediumRtParams;
import static org.corfudb.test.RtParamsForTest.getSmallRtParams;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

public class PersistentCorfuTableTest extends AbstractViewTest {

    private static final String INTERRUPTED_ERROR_MSG = "Unexpected InterruptedException";

    @Test
    public void testTxn() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        largeUuidManagedTable().execute((rt, corfuTable) -> {
            Uuid key1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
            Uuid payload1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
            Uuid metadata1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
            CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(payload1, metadata1);

            rt.getObjectsView().TXBegin();

            // Table should be empty
            assertThat(corfuTable.get(key1)).isNull();
            assertThat(corfuTable.size()).isZero();

            // Put key1
            corfuTable.insert(key1, value1);

            // Table should now have size 1 and contain key1
            assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());
            assertThat(corfuTable.get(key1).getPayload().getMsb()).isEqualTo(payload1.getMsb());
            assertThat(corfuTable.size()).isEqualTo(1);
            rt.getObjectsView().TXEnd();

            Uuid key2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
            Uuid payload2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
            Uuid metadata2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
            CorfuRecord<Uuid, Uuid> value2 = new CorfuRecord<>(payload2, metadata2);

            Uuid nonExistingKey = Uuid.newBuilder().setLsb(3).setMsb(3).build();

            rt.getObjectsView().TXBegin();

            // Put key2
            corfuTable.insert(key2, value2);

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

            // Verify the MVOCache has exactly 2 versions
            Set<VersionedObjectIdentifier> voIds = rt.getObjectsView().getMvoCache().keySet();
            assertThat(voIds).containsExactlyInAnyOrder(
                    new VersionedObjectIdentifier(corfuTable.getCorfuSMRProxy().getStreamID(), -1L),
                    new VersionedObjectIdentifier(corfuTable.getCorfuSMRProxy().getStreamID(), 0L));
        });
    }

    @Test
    public void simpleParallelAccess() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        smallUuidManagedTable().execute((rt, corfuTable) -> {
            int readSize = 100;

            // 1st txn at v0 puts keys {0, .., readSize-1} into the table
            rt.getObjectsView().TXBegin();
            for (int i = 0; i < readSize; i++) {
                Uuid key = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                Uuid payload = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                Uuid metadata = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                CorfuRecord<Uuid, Uuid> value = new CorfuRecord<>(payload, metadata);
                corfuTable.insert(key, value);
            }
            rt.getObjectsView().TXEnd();

            // 2nd txn at v1 puts keys {readSize, ..., readSize*2-1} into the table
            rt.getObjectsView().TXBegin();
            for (int i = readSize; i < 2 * readSize; i++) {
                Uuid key = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                Uuid payload = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                Uuid metadata = Uuid.newBuilder().setLsb(i).setMsb(i).build();
                CorfuRecord<Uuid, Uuid> value = new CorfuRecord<>(payload, metadata);
                corfuTable.insert(key, value);
            }
            rt.getObjectsView().TXEnd();

            // Two threads doing snapshot read in parallel
            Thread t1 = new Thread(() -> snapshotRead(rt, corfuTable, 0, 0, readSize));
            Thread t2 = new Thread(() -> snapshotRead(rt, corfuTable, 1, readSize, 2 * readSize));

            t1.start();
            t2.start();
            t1.join();
            t2.join();
        });
    }

    /**
     * Verify that a transaction does not observe uncommitted changes by another
     * parallel transaction.
     */
    @Test
    public void testUncommittedChangesIsolationBetweenParallelTxns() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getLargeRtParams(), rt -> {
            ManagedCorfuTable.buildDefault(rt).execute(corfuTable -> {
                Uuid key1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
                Uuid payload1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
                Uuid payload2 = Uuid.newBuilder().setLsb(2).setMsb(2).build();
                Uuid metadata1 = Uuid.newBuilder().setLsb(1).setMsb(1).build();
                CorfuRecord<Uuid, Uuid> value1 = new CorfuRecord<>(payload1, metadata1);

                // put(k1, v1)
                rt.getObjectsView().TXBegin();
                corfuTable.insert(key1, value1);
                rt.getObjectsView().TXEnd();
                assertThat(corfuTable.get(key1).getPayload().getLsb()).isEqualTo(payload1.getLsb());

                CountDownLatch readLatch = new CountDownLatch(1);
                CountDownLatch writeLatch = new CountDownLatch(1);
                AtomicLong readerResult = new AtomicLong();
                AtomicLong writerResult = new AtomicLong();

                Thread readerThread = new Thread(() -> {
                    rt.getObjectsView().TXBegin();
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
                    rt.getObjectsView().TXEnd();
                });

                // put(k1, v2) to overwrite the previous put, but do not commit
                Thread writerThread = new Thread(() -> {
                    rt.getObjectsView().TXBegin();
                    CorfuRecord<Uuid, Uuid> value2 = new CorfuRecord<>(payload2, metadata1);
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
                    rt.getObjectsView().TXEnd();
                });

                readerThread.start();
                writerThread.start();
                readerThread.join();
                writerThread.join();

                assertThat(readerResult.get()).isEqualTo(payload1.getLsb());
                assertThat(writerResult.get()).isEqualTo(payload2.getLsb());
            });
        });
    }

    /**
     * For MVO instances, the ObjectOpenOption.NO_CACHE should ensure that the instance
     * is not saved in ObjectsView.objectCache or MVOCache.objectCache
     */
    @Test
    public void testNoCacheOption() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getLargeRtParams(), rt -> {
            UUID streamA = UUID.randomUUID();
            UUID streamB = UUID.randomUUID();

            try (PersistentCorfuTable<String, String> tableA = rt.getObjectsView()
                    .build()
                    .setStreamID(streamA)
                    .setTypeToken(PersistentCorfuTable.<String, String>getTypeToken())
                    .addOpenOption(ObjectOpenOption.CACHE)
                    .open()) {

                try (PersistentCorfuTable<String, String> tableB = rt.getObjectsView()
                        .build()
                        .setStreamID(streamB)
                        .setTypeToken(PersistentCorfuTable.<String, String>getTypeToken())
                        .addOpenOption(ObjectOpenOption.NO_CACHE)
                        .open()) {

                    String key = "key";
                    String value = "value";

                    tableA.insert(key, value);
                    tableB.insert(key, value);

                    // Access the table and populate the cache
                    tableA.size();
                    tableB.size();

                    assertThat(rt.getObjectsView().getObjectCache())
                            .containsOnlyKeys(new ObjectID(streamA, PersistentCorfuTable.class));

                    Set<VersionedObjectIdentifier> allKeys = rt.getObjectsView().getMvoCache().keySet();
                    Set<UUID> allObjectIds = allKeys.stream()
                            .map(VersionedObjectIdentifier::getObjectId)
                            .collect(Collectors.toSet());
                    assertThat(allObjectIds).containsOnly(streamA);
                }
            }
        });
    }

    // PersistentCorfuTable SecondaryIndexes Tests - Adapted From CorfuTableTest & CorfuStoreSecondaryIndexTest

    /**
     * Verify that a lookup by index throws an exception,
     * when the index has never been specified for this PersistentCorfuTable.
     */
    @Test(expected = IllegalArgumentException.class)
    public void cannotLookupByIndexWhenIndexNotSpecified() {
        addSingleServer(SERVERS.PORT_0);

        ManagedCorfuTableConfig<Uuid, ExampleValue, ManagedMetadata> cfg = ManagedCorfuTableConfig
                .buildExampleVal(tableCfg -> tableCfg.withSchema(false));


        ManagedCorfuTable
                .from(cfg, ManagedRuntime.from(getSmallRtParams()))
                .tableSetup(ManagedCorfuTableSetupManager.persistentProtobufCorfu())
                .execute((rt, corfuTable) -> {
                    ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

                    for (long i = 0; i < CacheSizeForTest.SMALL.size; i++) {
                        rt.getObjectsView().TXBegin();
                        corfuTable.insert(Uuid.newBuilder().setLsb(i).setMsb(i).build(),
                                new CorfuRecord<>(ExampleValue.newBuilder()
                                        .setPayload("abc")
                                        .setAnotherKey(i)
                                        .build(), user_1)
                        );
                        rt.getObjectsView().TXEnd();
                    }

                    corfuTable.getByIndex(() -> "anotherKey", 0);
                });
    }

    private <T> List<T> toList(@Nonnull Iterable<T> iterable) {
        return StreamSupport
                .stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
    }

    /**
     * Verify that a lookup by index on an empty table returns empty.
     */
    @Test
    public void emptyIndexesReturnEmptyValues() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getMediumRtParams(), rt -> {
            ManagedCorfuTable.buildExample(rt).execute(corfuTable -> {
                rt.getObjectsView().TXBegin();
                List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                        entries = toList(corfuTable.getByIndex(() -> "anotherKey", 0));
                assertThat(entries).isEmpty();

                entries = toList(corfuTable.getByIndex(() -> "uuid", Uuid.getDefaultInstance()));
                assertThat(entries).isEmpty();
                rt.getObjectsView().TXEnd();
            });
        });
    }

    /**
     * Very basic functionality of secondary indexes.
     */
    @Test
    public void testSecondaryIndexesBasic() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getMediumRtParams(), rt -> {
            ManagedCorfuTable
                    .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(ExampleSchemas.Uuid.class)
                            .vClass(ExampleValue.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(corfuTable -> {
                        final UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
                        ExampleSchemas.Uuid key1 = ExampleSchemas.Uuid.newBuilder()
                                .setLsb(uuid1.getLeastSignificantBits()).setMsb(uuid1.getMostSignificantBits())
                                .build();

                        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();
                        final long eventTime = 123L;
                        final UUID randomUUID = UUID.randomUUID();
                        ExampleSchemas.Uuid secondaryKey1 = ExampleSchemas.Uuid.newBuilder()
                                .setLsb(randomUUID.getLeastSignificantBits())
                                .setMsb(randomUUID.getMostSignificantBits())
                                .build();

                        rt.getObjectsView().TXBegin();
                        corfuTable.insert(key1, new CorfuRecord<>(ExampleValue.newBuilder()
                                .setPayload("abc")
                                .setAnotherKey(eventTime)
                                .setUuid(secondaryKey1)
                                .build(), user_1));
                        rt.getObjectsView().TXEnd();

                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<ExampleSchemas.Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                                entries = toList(corfuTable.getByIndex(() -> "anotherKey", eventTime));

                        assertThat(entries).hasSize(1);
                        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
                        rt.getObjectsView().TXEnd();

                        rt.getObjectsView().TXBegin();
                        entries = toList(corfuTable.getByIndex(() -> "uuid", secondaryKey1));
                        assertThat(entries).hasSize(1);
                        assertThat(entries.get(0).getValue().getPayload().getPayload()).isEqualTo("abc");
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Verify that secondary indexes are updated on removes.
     */
    @Test
    public void doUpdateIndicesOnRemove() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(ExampleSchemas.Uuid.class)
                            .vClass(ExampleValue.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<ExampleSchemas.Uuid, ExampleValue, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
                        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();
                        final long numEntries = 10;

                        ArrayList<Map.Entry<ExampleSchemas.Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                                initialEntries = LongStream
                                .rangeClosed(1, numEntries)
                                .boxed()
                                .map(i -> {
                                    ExampleValue exampleValue = ExampleValue.newBuilder()
                                            .setPayload("abc")
                                            .setAnotherKey(i)
                                            .setUuid(ExampleSchemas.Uuid.getDefaultInstance())
                                            .build();
                                    return Pair.of(
                                            ExampleSchemas.Uuid.newBuilder().setLsb(i).setMsb(i).build(),
                                            new CorfuRecord<>(exampleValue, user_1)
                                    );
                                })
                                .collect(Collectors.toCollection(ArrayList::new));

                        assertEquals(numEntries, initialEntries.size());

                        // Insert entries into table
                        for (Map.Entry<ExampleSchemas.Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : initialEntries) {
                            rt.getObjectsView().TXBegin();
                            table.insert(entry.getKey(), entry.getValue());
                            rt.getObjectsView().TXEnd();
                        }

                        // Verify secondary indexes
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<ExampleSchemas.Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                                entries = toList(table.getByIndex(() -> "anotherKey", numEntries));
                        assertThat(entries).hasSize(1);
                        assertThat(entries.get(0).getKey().getLsb()).isEqualTo(numEntries);
                        assertThat(entries.get(0).getKey().getMsb()).isEqualTo(numEntries);
                        assertThat(entries.get(0).getValue().getPayload().getAnotherKey()).isEqualTo(numEntries);


                        entries = toList(table.getByIndex(() -> "uuid", ExampleSchemas.Uuid.getDefaultInstance()));
                        assertThat(entries.size()).isEqualTo(numEntries);
                        assertThat(entries.containsAll(initialEntries)).isTrue();
                        assertThat(initialEntries.containsAll(entries)).isTrue();
                        rt.getObjectsView().TXEnd();

                        // Remove entries whose key LSB (UUID) is odd
                        ArrayList<Map.Entry<ExampleSchemas.Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>>
                                expectedEntries = initialEntries.stream()
                                .filter(entry -> entry.getKey().getLsb() % 2 == 0)
                                .collect(Collectors.toCollection(ArrayList::new));

                        for (long i = 0; i < numEntries; i++) {
                            if (i % 2 != 0) {
                                rt.getObjectsView().TXBegin();
                                table.delete(ExampleSchemas.Uuid.newBuilder().setLsb(i).setMsb(i).build());
                                rt.getObjectsView().TXEnd();
                            }
                        }

                        // Verify secondary indexes
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "anotherKey", numEntries));
                        assertThat(entries).hasSize(1);
                        assertThat(entries.get(0).getKey().getLsb()).isEqualTo(numEntries);
                        assertThat(entries.get(0).getKey().getMsb()).isEqualTo(numEntries);
                        assertThat(entries.get(0).getValue().getPayload().getAnotherKey()).isEqualTo(numEntries);

                        entries = toList(table.getByIndex(() -> "anotherKey", 1L));
                        assertThat(entries).isEmpty();

                        entries = toList(table.getByIndex(() -> "uuid", ExampleSchemas.Uuid.getDefaultInstance()));
                        assertThat(entries.size()).isEqualTo(expectedEntries.size());
                        assertThat(entries.containsAll(expectedEntries)).isTrue();
                        assertThat(expectedEntries.containsAll(entries)).isTrue();
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Very functionality of nested secondary indexes.
     */
    @Test
    public void testNestedSecondaryIndexes() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable.buildExample(rt).execute(table -> {
                // Create 100 records.
                final int totalRecords = 100;
                final long even = 0L;
                final long odd = 1L;
                List<Long> evenRecordIndexes = new ArrayList<>();
                ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

                for (long i = 0; i < totalRecords; i++) {
                    if (i % 2 == 0) {
                        evenRecordIndexes.add(i);
                    }

                    UUID uuid = UUID.randomUUID();
                    Uuid key = Uuid.newBuilder()
                            .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                            .build();

                    rt.getObjectsView().TXBegin();

                    NonPrimitiveValue.Builder valBuilder = NonPrimitiveValue.newBuilder()
                            .setKey1Level1(i % 2 == 0 ? even : odd)
                            .setKey2Level1(NonPrimitiveNestedValue.newBuilder()
                                    .setKey1Level2(i < (totalRecords / 2) ? "lower half" : "upper half")
                                    .setLevelNumber(2)
                                    .build());
                    ExampleValue exampleValue = ExampleValue.newBuilder()
                            .setPayload("payload_" + i)
                            .setAnotherKey(System.currentTimeMillis())
                            .setEntryIndex(i)
                            .setNonPrimitiveFieldLevel0(valBuilder)
                            .build();
                    table.insert(key, new CorfuRecord<>(exampleValue, user));
                    rt.getObjectsView().TXEnd();
                }

                // Get by secondary index, retrieve from database all even entries.
                rt.getObjectsView().TXBegin();
                List<Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>>> entries = toList(table
                        .getByIndex(() -> "non_primitive_field_level_0.key_1_level_1", even));

                assertThat(entries.size()).isEqualTo(totalRecords / 2);

                for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : entries) {
                    assertThat(evenRecordIndexes).contains(entry.getValue().getPayload().getEntryIndex());
                    evenRecordIndexes.remove(entry.getValue().getPayload().getEntryIndex());
                }

                assertThat(evenRecordIndexes).isEmpty();
                rt.getObjectsView().TXEnd();

                // Get by secondary index from second level (nested), retrieve from database 'upper half'.
                rt.getObjectsView().TXBegin();
                entries = toList(table
                        .getByIndex(() -> "non_primitive_field_level_0.key_2_level_1.key_1_level_2", "upper half"));

                assertThat(entries.size()).isEqualTo(totalRecords / 2);
                long sum = 0;

                for (Map.Entry<Uuid, CorfuRecord<ExampleValue, ManagedMetadata>> entry : entries) {
                    sum += entry.getValue().getPayload().getEntryIndex();
                }

                // Assert sum of consecutive numbers of "upper half" match the expected value.
                assertThat(sum).isEqualTo(((totalRecords / 2) / 2) * ((totalRecords / 2) + (totalRecords - 1)));
                rt.getObjectsView().TXEnd();
            });
        });
    }

    /**
     * Verify the case of a nested secondary index on REPEATED fields followed by a REPEATED non-primitive
     * field which is directly the indexed value.
     */
    @Test
    public void testNestedSecondaryIndexesWhenIndexedIsNonPrimitiveAndRepeated() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<Uuid, Company, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<Uuid, Company, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(Company.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<Uuid, Company, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
                        final int totalCompanies = 100;
                        List<Department> departments = createApartments();
                        createOffices(departments, totalCompanies, table, rt);

                        // Get by secondary index, retrieve from database all Companies that have Department of type 1.
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<Uuid, CorfuRecord<Company, ManagedMetadata>>>
                                entries = toList(table.getByIndex(() -> "office.departments", departments.get(0)));
                        assertThat(entries.size()).isEqualTo(totalCompanies / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index, retrieve from database all Companies that have Department of Type 4 (all).
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "office.departments", departments.get(3)));
                        assertThat(entries.size()).isEqualTo(totalCompanies);
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    private List<Department> createApartments() {
        // Department 1 for office_A and office_C
        Department dpt_1 = Department.newBuilder()
                .addMembers(Member.newBuilder()
                        .addPhoneNumbers("111-111-1111")
                        .setName("Member_DPT1")
                        .build())
                .build();

        // Department 2 for office_B
        Department dpt_2 = Department.newBuilder()
                .addMembers(Member.newBuilder()
                        .addPhoneNumbers("222-222-2222")
                        .setName("Member_DPT2")
                        .build())
                .build();

        // Department 3 for office_B
        Department dpt_3 = Department.newBuilder()
                .addMembers(Member.newBuilder()
                        .addPhoneNumbers("333-333-3333")
                        .setName("Member_DPT3")
                        .build())
                .build();

        // Department 4 for all offices
        Department dpt_4 = Department.newBuilder()
                .addMembers(Member.newBuilder()
                        .addPhoneNumbers("444-444-4444")
                        .setName("Member_DPT4")
                        .build())
                .build();

        return Arrays.asList(dpt_1, dpt_2, dpt_3, dpt_4);
    }

    private void createOffices(
            List<Department> departments, int totalCompanies,
            ICorfuTable<Uuid, CorfuRecord<Company, ManagedMetadata>> table, CorfuRuntime rt) {
        // Even indexed companies will have Office_A and Office_C
        Office office_A = Office.newBuilder()
                .addDepartments(departments.get(0))
                .addDepartments(departments.get(3))
                .build();

        // Odd indexed companies will have Office_B
        Office office_B = Office.newBuilder()
                .addDepartments(departments.get(1))
                .addDepartments(departments.get(2))
                .addDepartments(departments.get(3))
                .build();

        Office office_C = Office.newBuilder()
                .addDepartments(departments.get(0))
                .addDepartments(departments.get(3))
                .build();

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < totalCompanies; i++) {
            UUID id = UUID.randomUUID();
            Uuid networkId = Uuid.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            rt.getObjectsView().TXBegin();
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
            rt.getObjectsView().TXEnd();
        }
    }

    /**
     * Verify that nested secondary indexes work on repeated fields when the repeated field is
     * not the root level but a nested level.
     */
    @Test
    public void testNestedSecondaryIndexesNestedRepeatedField() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<Uuid, Person, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<Uuid, Person, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(Person.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<Uuid, Person, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
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

                            rt.getObjectsView().TXBegin();
                            Person person = Person.newBuilder()
                                    .setName("Name_" + i)
                                    .setAge(i)
                                    .setPhoneNumber(PhoneNumber.newBuilder()
                                            .setHome(UUID.randomUUID().toString())
                                            .addMobile(i % 2 == 0 ? mobileForEvens : mobileForOdds)
                                            .addMobile(mobileCommonBoth)
                                            .build())
                                    .build();
                            table.insert(key, new CorfuRecord<>(person, user));
                            rt.getObjectsView().TXEnd();
                        }

                        // Get by secondary index, retrieve from database all even entries.
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<Uuid, CorfuRecord<Person, ManagedMetadata>>>
                                entries = toList(table.getByIndex(() -> "phoneNumber.mobile", mobileForEvens));
                        assertThat(entries.size()).isEqualTo(people / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index, retrieve from database all entries with common mobile number.
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "phoneNumber.mobile", mobileCommonBoth));
                        assertThat(entries.size()).isEqualTo(people);
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Verify that nested secondary indexes work on recursive 'repeated' fields.
     */
    @Test
    public void testNestedSecondaryIndexesRecursiveRepeatedFields() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<Uuid, Office, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<Uuid, Office, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(Office.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<Uuid, Office, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
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

                            rt.getObjectsView().TXBegin();
                            Office office = Office.newBuilder()
                                    // Department 1 per Office
                                    .addDepartments(Department.newBuilder()
                                            // Department 1 - Member 1
                                            .addMembers(Member.newBuilder()
                                                    .setName("Office_" + i + "_Dpt.1_Member_1")
                                                    .addPhoneNumbers(i % 2 == 0 ? evenPhoneNumber : oddPhoneNumber)
                                                    .addPhoneNumbers(homePhoneNumber)
                                                    .addPhoneNumbers(commonPhoneNumber)
                                                    .build())
                                            // Department 1 - Member 2
                                            .addMembers(Member.newBuilder()
                                                    .setName("Office_" + i + "_Dpt.1_Member_2")
                                                    .addPhoneNumbers(commonPhoneNumber)
                                                    .build())
                                            .build())
                                    // Department 2 per Office
                                    .addDepartments(Department.newBuilder()
                                            // Department 2 - Member 1
                                            .addMembers(Member.newBuilder()
                                                    .setName("Office_" + i + "_Dpt.2_Member_1")
                                                    .addPhoneNumbers(commonPhoneNumber)
                                                    .build())
                                            .build())
                                    .build();
                            table.insert(officeId, new CorfuRecord<>(office, user));
                            rt.getObjectsView().TXEnd();
                        }

                        // Get by secondary index, retrieve from database all offices which have an evenPhoneNumber.
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<Uuid, CorfuRecord<Office, ManagedMetadata>>> entries = toList(table
                                .getByIndex(() -> "departments.members.phoneNumbers", evenPhoneNumber));

                        assertThat(entries.size()).isEqualTo(numOffices / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index, retrieve from database all entries with common mobile number.
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "departments.members.phoneNumbers", commonPhoneNumber));
                        assertThat(entries.size()).isEqualTo(numOffices);
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Verify that we can access a secondary index based on a custom alias or the default alias.
     */
    @Test
    public void testSecondaryIndexAlias() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<Uuid, Adult, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<Uuid, Adult, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(Adult.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<Uuid, Adult, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
                        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();
                        final int adultCount = 50;
                        final long adultBaseAge = 30L;
                        final long kidsBaseAge = 4L;

                        for (int i = 0; i < adultCount; i++) {
                            UUID adultId = UUID.randomUUID();
                            Uuid adultKey = Uuid.newBuilder()
                                    .setMsb(adultId.getMostSignificantBits()).setLsb(adultId.getLeastSignificantBits())
                                    .build();

                            rt.getObjectsView().TXBegin();
                            final long adultAge = i % 2 == 0 ? adultBaseAge : adultBaseAge * 2;
                            final long kidsAge = i % 2 == 0 ? kidsBaseAge : kidsBaseAge * 2;

                            Person person = Person.newBuilder()
                                    .setName("Name_" + i)
                                    .setAge(adultAge)
                                    .setPhoneNumber(PhoneNumber.newBuilder()
                                            .setHome(UUID.randomUUID().toString())
                                            .build())
                                    .setChildren(Children.newBuilder()
                                            .addChild(Child.newBuilder()
                                                    .setName("Child_" + i)
                                                    .setAge(kidsAge)
                                            ).build()
                                    )
                                    .build();
                            Adult adult = Adult.newBuilder()
                                    .setPerson(person)
                                    .build();
                            table.insert(adultKey, new CorfuRecord<>(adult, user));
                            rt.getObjectsView().TXEnd();
                        }

                        // Get by secondary index (default alias),
                        // retrieve from database all adults with adultsBaseAge.
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<Uuid, CorfuRecord<Adult, ManagedMetadata>>>
                                entries = toList(table.getByIndex(() -> "age", adultBaseAge));
                        assertThat(entries.size()).isEqualTo(adultCount / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index (using fully qualified name),
                        // retrieve from database all adults with adultsBaseAge.
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "person.age", adultBaseAge));
                        assertThat(entries.size()).isEqualTo(adultCount / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index (custom alias),
                        // retrieve from database all adults with kids on age 'kidsBaseAge'.
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "kidsAge", kidsBaseAge));
                        assertThat(entries.size()).isEqualTo(adultCount / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index (fully qualified name),
                        // retrieve from database all adults with kids on age 'kidsBaseAge'.
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "person.children.child.age", kidsBaseAge));
                        assertThat(entries.size()).isEqualTo(adultCount / 2);
                        rt.getObjectsView().TXEnd();

                        // Get by secondary index (custom alias),
                        // retrieve from database all adults with kids on age '2' (non-existent).
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "kidsAge", 2));
                        assertThat(entries.size()).isZero();
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Test indexing of 'NULL' (i.e., unset non-primitive sub-fields) for the following sub-field patterns
     * (from the root):
     * <p>
     * Refer to SportsProfessional proto, in 'example_schemas.proto' for definitions.
     * <p>
     * (1) Repeated field followed by oneOf field (e.g., hobby.sport)
     * (2) Non-repeated field followed by oneOf field (e.g., profession.sport)
     * (3) Repeated field followed by repeated field (e.g., training.exercises)
     */
    @Test
    public void testNestedIndexesWithNullValues() throws Exception {
        addSingleServer(SERVERS.PORT_0);
        buildNewManagedRuntime(getSmallRtParams(), rt -> {
            ManagedCorfuTable
                    .<Uuid, SportsProfessional, ManagedMetadata>builder()
                    .config(ManagedCorfuTableConfig
                            .<Uuid, SportsProfessional, ManagedMetadata>builder()
                            .rt(rt)
                            .kClass(Uuid.class)
                            .vClass(SportsProfessional.class)
                            .mClass(ManagedMetadata.class)
                            .build()
                    )
                    .tableSetup(new ManagedCorfuTableSetupManager<Uuid, SportsProfessional, ManagedMetadata>()
                            .getPersistentProtobufCorfu()
                    )
                    .build()
                    .execute(table -> {
                        // Define a player and set only (1) oneOf type, then query for the unset field to confirm this
                        // is indexed as NULL (i.e., not set)
                        Hobby profession = Hobby.newBuilder()
                                .setBasket(Basketball.newBuilder().setTeam("Chicago Bulls").build())
                                .build();
                        Hobby hobby = Hobby.newBuilder()
                                .setBaseball(Baseball.newBuilder().build())
                                .build();
                        SportsProfessional player1 = SportsProfessional.newBuilder()
                                .setPerson(Person.newBuilder().setName("Michael Jordan").build())
                                // Set Basket as profession (oneOf field) so query for Baseball as profession
                                .setProfession(profession)
                                // Set Baseball as hobby (oneOf field) so query for Basket as hobby
                                .addHobby(hobby)
                                // Do not define any sub-field of repeated type (Exercises)
                                // and confirmed its indexed as NULL
                                .addTraining(TrainingPlan.newBuilder().build())
                                .build();

                        // Define a player which does not have any indexed sub-field set
                        // (therefore, it should be indexed as NULL)
                        SportsProfessional playerUndefined = SportsProfessional.newBuilder()
                                .setPerson(Person.newBuilder().setName("Undefined").build())
                                // Don't set any 'oneOf' sport for profession (sub-field)
                                .setProfession(Hobby.newBuilder().build())
                                // Don't set any 'oneOf' sport for Hobby (sub-field)
                                .addHobby(hobby)
                                // Do not define any sub-field of repeated type (Exercises)
                                // and confirmed its indexed as NULL
                                .addTraining(TrainingPlan.newBuilder().build())
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

                        rt.getObjectsView().TXBegin();
                        table.insert(idPlayer1, new CorfuRecord<>(
                                player1,
                                ManagedMetadata.newBuilder().setCreateUser("user_UT").build()
                        ));

                        table.insert(idPlayerUndefined, new CorfuRecord<>(
                                playerUndefined,
                                ManagedMetadata.newBuilder().setCreateUser("user_UT").build()
                        ));
                        rt.getObjectsView().TXEnd();

                        // Query secondary indexes
                        // (1) Repeated field followed by oneOf field (e.g., hobby.sport)
                        rt.getObjectsView().TXBegin();
                        List<Map.Entry<Uuid, CorfuRecord<SportsProfessional, ManagedMetadata>>>
                                entries = toList(table.getByIndex(() -> "basketAsHobby", null));
                        assertThat(entries.size()).isEqualTo(2);
                        rt.getObjectsView().TXEnd();

                        // (2) Non-repeated field followed by oneOf field (e.g., profession.sport)
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "baseballPlayers", null));
                        assertThat(entries.size()).isEqualTo(2);
                        rt.getObjectsView().TXEnd();

                        // (3) Repeated field followed by repeated field (e.g., training.exercises)
                        rt.getObjectsView().TXBegin();
                        entries = toList(table.getByIndex(() -> "exercises", null));
                        assertThat(entries.size()).isEqualTo(2);
                        rt.getObjectsView().TXEnd();
                    });
        });
    }

    /**
     * Test that a table without any updates can be served when a concurrent
     * transaction syncs the stream forward before this first transaction has
     * a chance to request a snapshot proxy.
     */
    @Test
    public void testTableNoUpdateInterleave() {
        SMRObject.Builder<PersistentCorfuTable<String, String>> t1Builder = getTableBuilder(getDefaultRuntime(), "t1");
        SMRObject.Builder<PersistentCorfuTable<String, String>> t2Builder = getTableBuilder(getDefaultRuntime(), "t2");

        try (PersistentCorfuTable<String, String> table1 = t1Builder.open()) {
            try (PersistentCorfuTable<String, String> table2 = t2Builder.open()) {

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
        }
    }

    private SMRObject.Builder<PersistentCorfuTable<String, String>> getTableBuilder(CorfuRuntime rt, String streamName) {
        return rt
                .getObjectsView()
                .build()
                .setTypeToken(PersistentCorfuTable.<String, String>getTypeToken())
                .setStreamName(streamName);
    }

    /**
     * Validate that the state of the underlying object is reset when an exception occurs
     * during the sync process. Subsequent reads operations should succeed and not see
     * incomplete or stale data.
     */
    @Test
    public void validateObjectAfterExceptionDuringSync() throws Exception {
        addSingleServer(SERVERS.PORT_0);

        buildNewManagedRuntime(getLargeRtParams(), rt -> {
            try (PersistentCorfuTable<String, String> table1 = getTableBuilder(rt, "t1").open()) {
                // Populate the table with initial entries.
                final int numEntries = 100;
                for (int i = 0; i < numEntries; i++) {
                    rt.getObjectsView().TXBegin();
                    table1.insert(Integer.toString(i), Integer.toString(i));
                    rt.getObjectsView().TXEnd();
                }

                rt.shutdown();

                final CorfuRuntime spyRt = spy(getDefaultRuntime());
                final AddressSpaceView spyAddressSpaceView = spy(new AddressSpaceView(spyRt));
                final Long triggerAddress = 80L;

                // Mock the AddressSpace behaviour so that an exception can be thrown
                // midway through the table sync process.
                doReturn(spyAddressSpaceView).when(spyRt).getAddressSpaceView();
                doThrow(new UnreachableClusterException("Cluster is unreachable"))
                        .when(spyAddressSpaceView)
                        .read(eq(triggerAddress), any(), any());

                try (PersistentCorfuTable<String, String> spyTable = spyRt.getObjectsView().build()
                        .setTypeToken(PersistentCorfuTable.<String, String>getTypeToken())
                        .setStreamName("t1")
                        .open()) {
                    Exception triggeredException = null;

                    // Attempt a read. This will trigger a sync from the fresh runtime and throw
                    // the above exception after reading address 80.
                    try {
                        spyRt.getObjectsView().TXBegin();
                        spyTable.size();
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
                        assertThat(spyTable.size()).isEqualTo(numEntries);

                        for (int i = 0; i < numEntries; i++) {
                            assertThat(spyTable.get(Integer.toString(i)))
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
        });
    }

    private void snapshotRead(
            CorfuRuntime rt, GenericCorfuTable<?, Uuid, CorfuRecord<Uuid, Uuid>> corfuTable,
            long ts, int low, int high) {
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, ts))
                .build()
                .begin();
        for (int i = low; i < high; i++) {
            Uuid key = Uuid.newBuilder().setLsb(i).setMsb(i).build();
            assertThat(corfuTable.get(key).getPayload().getLsb()).isEqualTo(i);
            assertThat(corfuTable.get(key).getPayload().getMsb()).isEqualTo(i);
        }
        rt.getObjectsView().TXEnd();
    }

    private ManagedCorfuTable<Uuid, Uuid, Uuid> smallUuidManagedTable() {
        return managedTable(getSmallRtParams());
    }

    private ManagedCorfuTable<Uuid, Uuid, Uuid> largeUuidManagedTable() {
        return managedTable(getLargeRtParams());
    }

    private ManagedCorfuTable<Uuid, Uuid, Uuid> managedTable(CorfuRuntimeParameters rtParams) {
        ManagedRuntime managedRt = ManagedRuntime
                .from(rtParams)
                .setup(rt -> rt.parseConfigurationString(getDefaultConfigurationString()));

        return ManagedCorfuTable
                .<Uuid, Uuid, Uuid>build()
                .config(ManagedCorfuTableConfig.buildUuid())
                .managedRt(managedRt)
                .tableSetup(ManagedCorfuTableSetupManager.persistentProtobufCorfu());
    }
}
