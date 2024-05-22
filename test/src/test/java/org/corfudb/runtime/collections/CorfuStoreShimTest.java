package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuOptions.PersistenceOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.test.SampleSchema;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.Options;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static com.google.protobuf.Descriptors.FileDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.runtime.object.PersistenceOptions.DISABLE_BLOCK_CACHE;
import static org.corfudb.runtime.object.PersistenceOptions.disposeBlockCache;
import static org.corfudb.runtime.object.PersistenceOptions.newBlockCache;
import static org.corfudb.test.SampleAppliance.Appliance;
import static org.corfudb.test.SampleSchema.FirewallRule;
import static org.corfudb.test.SampleSchema.ManagedResources;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * To ensure that feature changes in CorfuStore do not break verticals,
 * we simulate their usage pattern with implementation and tests.
 * <p>
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class CorfuStoreShimTest extends AbstractViewTest {
    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
    }

    /**
     * CorfuStoreShim supports read your transactional writes implicitly when reads
     * happen in a write transaction or vice versa
     * This test demonstrates how that would work
     *
     * @throws Exception exception
     */
    @Test
    public void checkDirtyReads() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        UuidMsg key1 = UuidMsg.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        ManagedTxnContext txn = shimStore.tx(someNamespace);
        txn.putRecord(tableName, key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                user_1);
        CorfuStoreMetadata.Timestamp timestamp = txn.commit();
        long tail1 = shimStore.getHighestSequence(someNamespace, tableName);

        // Take a snapshot to test snapshot isolation transaction
        CorfuStoreEntry<UuidMsg, ManagedMetadata, ManagedMetadata> entry;
        // Start a dirty read transaction
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            readWriteTxn.putRecord(table, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().build());

            // Now within the same txn query the object and validate that it shows the local update.
            entry = readWriteTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getCreateUser()).isEqualTo("xyz");
            readWriteTxn.commit();
        }

        long tail2 = shimStore.getHighestSequence(someNamespace, tableName);
        assertThat(tail2).isGreaterThan(tail1);

        // Try a read followed by write in same txn
        // Start a dirty read transaction
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            entry = readWriteTxn.getRecord(table, key1);
            readWriteTxn.putRecord(table, key1,
                    ManagedMetadata.newBuilder()
                            .setCreateUser("abc" + entry.getPayload().getCreateUser())
                            .build(),
                    ManagedMetadata.newBuilder().build());
            readWriteTxn.commit();
        }

        // Try a read on an older timestamp
        try (ManagedTxnContext readTxn = shimStore.tx(someNamespace, IsolationLevel.snapshot(timestamp))) {
            entry = readTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getCreateUser()).isEqualTo("abc");
        }
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            UuidMsg key2 = null;
            assertThatThrownBy(() -> readWriteTxn.putRecord(tableName, key2, null, null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        // Validate conflict aborts have full tablename in message
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            readWriteTxn.putRecord(table, key1, user_1, user_1);
            CompletableFuture.runAsync(() -> {
                ManagedTxnContext anotherTxn = shimStore.tx(someNamespace);
                anotherTxn.putRecord(table, key1, user_1, user_1);
                anotherTxn.commit();
            }).get();
            readWriteTxn.commit();
        } catch (TransactionAbortedException ex) {
            assertThat(ex.getMessage()).contains(table.getFullyQualifiedTableName());
        }
    }

    /**
     * Test that freeTableData works and removes table from cache
     * Also validates that the subscribeListener() does not throw any exceptions
     * if called after freeTableData() is invoked.
     *
     * @throws Exception exception
     */
    @Test
    public void checkFreeTableData() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ExampleSchemas.ClusterUuidMsg, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.ClusterUuidMsg.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
            UuidMsg key = UuidMsg.newBuilder().setMsb(i)
                    .build();
            ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

            ManagedTxnContext txn = shimStore.tx(someNamespace);
            txn.putRecord(table, key,
                    ExampleSchemas.ClusterUuidMsg.newBuilder().setMsb(i).build(),
                    user_1);
            txn.commit();
        }

        assertThatThrownBy(() -> shimStore.freeTableData("non", "existent"))
                .isExactlyInstanceOf(NoSuchElementException.class);

        // Release all the cached objects from the insertions above
        shimStore.freeTableData(someNamespace, tableName);

        Set<VersionedObjectIdentifier> allKeys = corfuRuntime.getObjectsView().getMvoCache().keySet();
        assertThat(allKeys.stream().map(VersionedObjectIdentifier::getObjectId).collect(Collectors.toSet()))
                .isNotEmpty()
                .doesNotContain(table.getStreamUUID());

        class EmptyStreamListener implements StreamListener {
            @Override
            public void onNext(CorfuStreamEntries results) {
            }
            @Override
            public void onError(Throwable throwable) {
            }
        }
        EmptyStreamListener emptyStreamListener = new EmptyStreamListener();

        // verify that subscription does not throw any exceptions because the table was closed
        shimStore.subscribeListener(emptyStreamListener, someNamespace, "cluster_manager_test");

        // Now simple access the table after free to validate that it works
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
            UuidMsg key = UuidMsg.newBuilder().setMsb(i)
                    .build();
            ManagedTxnContext txn = shimStore.tx(someNamespace);
            CorfuStoreEntry<UuidMsg, ExampleSchemas.ClusterUuidMsg, ManagedMetadata> record = txn.getRecord(table, key);
            assertThat(record.getPayload()).isNotNull();
            assertThat(record.getPayload().getMsb()).isEqualTo(i);
            txn.commit();
        }

        // By some future bug should the table disappear from the object cache ensure that
        // the method still fails gracefully with a NoSuchElementException
        ObjectID oid = new ObjectID(table.getStreamUUID(), PersistentCorfuTable.class);
        corfuRuntime.getObjectsView().getObjectCache().remove(oid);
        assertThatThrownBy(() -> shimStore.freeTableData(someNamespace, tableName))
                .isExactlyInstanceOf(NoSuchElementException.class);
    }

    /**
     * CorfuStore stores 3 pieces of information - key, value and metadata
     * This test demonstrates how metadata field options especially "version" can be used and verified.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources> table = shimStore.openTable(
                nsxManager,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        SampleSchema.Uuid key1 = SampleSchema.Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedResources user_1 = ManagedResources.newBuilder().setCreateUser("user_1").build();
        ManagedResources user_2 = ManagedResources.newBuilder().setCreateUser("user_2").build();
        long expectedVersion = 0L;

        try (ManagedTxnContext txn = shimStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("abc").build(), user_1);
            txn.commit();
        }

        assertThat(shimStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_1")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Set the version field to the correct value 1 and expect that no exception is thrown
        try (ManagedTxnContext txn = shimStore.tx(nsxManager)) {
            // Enforce version number
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("bcd").build(),
                    ManagedResources.newBuilder().setCreateUser("user_2").setVersion(0L).build());
            txn.commit();
        }

        // Honor enforced version number
        assertThat(shimStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(0L).build());

        // Now do an update without setting the version field, and it should not get validated!
        try (ManagedTxnContext txn = shimStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("cde").build(),
                    user_2);
            txn.commit();
        }

        assertThat(shimStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        try (ManagedTxnContext txn = shimStore.tx(nsxManager)) {
            txn.delete(table, key1);
            txn.commit();
        }
        assertThat(shimStore.getTable(nsxManager, tableName).get(key1)).isNull();
        expectedVersion = 0L;

        try (ManagedTxnContext txn = shimStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("def").build(), user_2);
            txn.commit();
        }

        assertThat(shimStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder(user_2)
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Verify the table is readable using entryStream()
        final int batchSize = 50;
        Stream<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources> entry : partition) {
                assertThat(entry.getKey()).isExactlyInstanceOf(SampleSchema.Uuid.class);
                assertThat(entry.getPayload()).isExactlyInstanceOf(SampleSchema.EventInfo.class);
                assertThat(entry.getMetadata()).isExactlyInstanceOf(ManagedResources.class);
            }
        }
    }

    /**
     * Demonstrates that opening same table from multiple threads will retry internal transactions
     *
     * @throws Exception
     */
    @Test
    public void checkOpenRetriesTXN() throws Exception {
        CorfuRuntime corfuRuntime = getDefaultRuntime();
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);
        final String nsxManager = "nsx-manager"; // namespace for the table
        final String tableName = "EventInfo"; // table name
        final int numThreads = 5;
        scheduleConcurrently(numThreads, t -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                // Create & Register the table.
                // This is required to initialize the table for the current corfu client.
                shimStore.openTable(nsxManager, tableName, SampleSchema.Uuid.class, SampleSchema.EventInfo.class, null,
                        TableOptions.builder().build());
            }

        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);
    }

    /**
     * CorfuStore uses WriteAfterWrite Transactions which detect conflicts only on concurrent writes.  For example,
     * TX1: reads key K (may also perform other operations on other keys and/or tables)
     * TX2: updates key K concurrently.
     * When TX1 commits, it will not hit a TransacationAbortedException.  To consider K for conflict resolution, TX1
     * must touch() it if no other updates are made to it.
     *
     * This test runs the above 2 transactions concurrently for 100 iterations and verifies that
     * TransactionAbortedException gets thrown at least once if the key is touched.
     * Additionally, it verifies that the exception is not thrown if the key is not touched.
     * @throws Exception
     */
    @Test
    public void testTouchReqdForReadTxWriteTxConflict() throws Exception {
        final int numIterations = 100;
        Assert.assertFalse(runReadAndWriteTxConcurrently(true, numIterations));
        Assert.assertTrue(runReadAndWriteTxConcurrently(false, numIterations));
    }

    private boolean runReadAndWriteTxConcurrently(boolean touch, int numIterations) throws Exception {
        final String namespace = "test_namespace";
        final String tableName1 = "EventInfo1";
        final String tableName2 = "EventInfo2";
        final int numThreads = 2;
        final int numRecords = 3;

        // Java allows only final variables in a lambda expression.  Use AtomicBoolean so that it can be modified from
        // the lambda expression.
        AtomicBoolean success = new AtomicBoolean(true);

        CorfuRuntime corfuRuntime = getDefaultRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Open 2 tables with the same schema
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, Message> table1 = corfuStore.openTable(namespace, tableName1,
            SampleSchema.Uuid.class, SampleSchema.EventInfo.class, null, TableOptions.builder().build());

        Table<SampleSchema.Uuid, SampleSchema.EventInfo, Message> table2 = corfuStore.openTable(namespace, tableName2,
            SampleSchema.Uuid.class, SampleSchema.EventInfo.class, null, TableOptions.builder().build());

        // Construct the conflict key
        SampleSchema.Uuid conflictKey = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();

        // In each iteration, write 3 records in table1.  Then start 2 transactions concurrently.  TX1 reads the
        // conflict key from table1, touches it(touch == true) and writes it to table2.  TX2 updates the conflict key
        // in table1.
        // The expected behavior is that TX1 should hit a TransactionAbortedException in at least 1 iteration if the
        // conflict key is touched.
        for (int i = 0; i < numIterations; i++) {
            // Write sample records in table1
            for (int j = 0; j < numRecords; j++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(j).setMsb(j).build();
                SampleSchema.EventInfo value = SampleSchema.EventInfo.newBuilder().setName("test" + j).build();
                try (TxnContext txnContext = corfuStore.txn(namespace)) {
                    txnContext.putRecord(table1, key, value, null);
                    txnContext.commit();
                }
            }

            // TX1
            scheduleConcurrently(f -> {
                try (TxnContext txnContext = corfuStore.txn(namespace)) {
                    SampleSchema.EventInfo value =
                        (SampleSchema.EventInfo) txnContext.getRecord(tableName1, conflictKey).getPayload();
                    if (touch) {
                        txnContext.touch(tableName1, conflictKey);
                    }
                    txnContext.putRecord(table2, conflictKey, value, null);
                    txnContext.commit();
                } catch (TransactionAbortedException tae) {
                    success.set(false);
                }
            });

            // TX2 updates the conflict key
            int index = i;
            SampleSchema.EventInfo newVal = SampleSchema.EventInfo.newBuilder().setName("test_new" + index).build();
            scheduleConcurrently(f -> {
                try {
                    IRetry.build(IntervalRetry.class, () -> {
                        try (TxnContext txnContext = corfuStore.txn(namespace)) {
                            txnContext.putRecord(table1, conflictKey, newVal, null);
                            txnContext.commit();
                        } catch (TransactionAbortedException tae) {
                            throw new RetryNeededException();
                        }
                        return null;
                    }).run();
                } catch (Exception e) {
                    log.error("Could not update the conflict key", e);
                }
            });

            executeScheduled(numThreads, PARAMETERS.TIMEOUT_NORMAL);

            // Verify that the write in TX2 was successful
            try (TxnContext txnContext = corfuStore.txn(namespace)) {
                Assert.assertEquals(newVal, txnContext.getRecord(tableName1, conflictKey).getPayload());
                assertThat(txnContext.getTxnSequence()).isNotEqualTo(Token.UNINITIALIZED.getSequence());
                assertThat(txnContext.getEpoch()).isNotEqualTo(Token.UNINITIALIZED.getEpoch());
                txnContext.commit();
            }

            // Clear the tables after each iteration
            table1.clearAll();
            table2.clearAll();
        }
        return success.get();
    }

    /**
     * This test verifies getHighestSequenceNumber is able to return the highest sequence number
     * corresponding to a DATA entry when the tail of a stream is filled with HOLES.
     *
     * @throws Exception
     */
    @Test
    public void getHighestSequenceNumberPresenceOfHoles() throws Exception {
        final String namespace = "corfu";
        final String tableName = "UT-Table";
        final int numUpdates = 30;
        final int numHoles = 10;

        // Open 'table' and write 'n' consecutive updates
        CorfuRuntime corfuRuntime = getTestRuntime();
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);
        Table<UuidMsg, ExampleSchemas.ExampleValue, ManagedMetadata> table = shimStore.openTable(
                namespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        UuidMsg key;
        UUID uuid;

        for (int index = 0; index < numUpdates; index++) {
            try (ManagedTxnContext writeTxn = shimStore.tx(namespace)) {
                uuid = UUID.nameUUIDFromBytes(String.valueOf(index).getBytes());
                key = UuidMsg.newBuilder().setLsb(uuid.getLeastSignificantBits()).setMsb(uuid.getMostSignificantBits()).build();
                writeTxn.putRecord(table, key,
                        ExampleSchemas.ExampleValue.newBuilder().setEntryIndex(index).build(),
                        ManagedMetadata.newBuilder().build());
                writeTxn.commit();
            }
        }

        // Verify Data is reflected in the sequencer
        StreamAddressSpace addressSpace = corfuRuntime.getSequencerView().getStreamAddressSpace(new StreamAddressRange(table.getStreamUUID(), Address.MAX, Address.NON_ADDRESS));
        assertThat(addressSpace.size()).isEqualTo(numUpdates);

        // Enforce 'numHoles' holes to 'table'
        for (int index = 0; index < numHoles; index++) {
            TokenResponse token = corfuRuntime.getSequencerView().next(table.getStreamUUID());
            corfuRuntime.getLayoutView().getRuntimeLayout().getLogUnitClient(SERVERS.ENDPOINT_0).write(LogData.getHole(token.getSequence())).get();
        }

        // Verify Data is reflected in the sequencer
        StreamAddressSpace addressSpaceWithHoles = corfuRuntime.getSequencerView().getStreamAddressSpace(new StreamAddressRange(table.getStreamUUID(), Address.MAX, Address.NON_ADDRESS));
        assertThat(addressSpaceWithHoles.size()).isEqualTo(numUpdates + numHoles);

        // Get Highest Sequence Number and verify it does not retrieve non-data entry.
        // Clear cache so we guarantee we are going to the server to read data
        corfuRuntime.getObjectsView().getObjectCache().clear();

        assertThat(shimStore.getHighestSequence(namespace, tableName)).isEqualTo(addressSpace.getTail());
    }

    /**
     * CorfuStoreShim stores 3 pieces of information - key, value and metadata
     * This test demonstrates how metadata field options esp "version" can be used and verified.
     *
     * @throws Exception exception
     */
    @Test
    public void checkRevisionValidation() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        UuidMsg key1 = UuidMsg.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                    user_1);
            txn.commit();
        }

        // Validate that touch() does not change the revision
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.touch(tableName, key1);
            txn.commit();
        }

        CorfuStoreEntry<UuidMsg, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext queryTxn = shimStore.tx(someNamespace)) {
            entry = queryTxn.getRecord(table, key1);
        }
        assertNotNull(entry);
        assertThat(entry.getMetadata().getRevision()).isEqualTo(0L);
        assertThat(entry.getMetadata().getCreateTime()).isLessThan(System.currentTimeMillis());

        // Ensure that if metadata's revision field is set, it is validated and exception thrown if stale
        final ManagedTxnContext txn1 = shimStore.tx(someNamespace);
        assertThatThrownBy(() -> txn1.putRecord(tableName, key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                ManagedMetadata.newBuilder().setRevision(1L).build()))
                .isExactlyInstanceOf(StaleRevisionUpdateException.class);

        // Correct revision field set should NOT throw an exception
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().setRevision(0L).build());
            txn.commit();
        }

        // Revision field not set should also not throw an exception, just internally bump up revision
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ManagedMetadata.newBuilder().setCreateUser("xyz").build(),
                    ManagedMetadata.newBuilder().build());
            txn.commit();
        }

        try (ManagedTxnContext queryTxn = shimStore.tx(someNamespace)) {
            entry = queryTxn.getRecord(table, key1);
        }
        assertThat(entry.getMetadata().getRevision()).isEqualTo(2L);
        assertThat(entry.getMetadata().getCreateUser()).isEqualTo("user_1");
        assertThat(entry.getMetadata().getLastModifiedTime()).isLessThan(System.currentTimeMillis() + 1);
        assertThat(entry.getMetadata().getCreateTime()).isLessThan(entry.getMetadata().getLastModifiedTime());
    }

    /**
     * Demonstrates the checks that the metadata passed into CorfuStoreShim is validated against.
     *
     * @throws Exception exception
     */
    @Test
    public void checkNullMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table =
                shimStore.openTable(
                        someNamespace,
                        tableName,
                        UuidMsg.class,
                        ManagedMetadata.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        UuidMsg key1 = UuidMsg.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
        ManagedTxnContext txn = shimStore.tx(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                null);
        txn.commit();
        txn = shimStore.tx(someNamespace);
        txn.putRecord(table,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                null);
        txn.commit();
        Message metadata = shimStore.getTable(someNamespace, tableName).get(key1).getMetadata();
        assertThat(metadata).isNull();

        // TODO: Finalize behavior of the following api with consumers..
        // Setting metadata into a schema that did not have metadata specified?
        txn = shimStore.tx(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("bcd").build(),
                ManagedMetadata.newBuilder().setCreateUser("testUser").setRevision(1L).build());
        txn.commit();
        assertThat(shimStore.getTable(someNamespace, tableName).get(key1).getMetadata())
                .isNotNull();

        // Now setting back null into the metadata which had non-null value
        txn = shimStore.tx(someNamespace);
        txn.putRecord(tableName,
                key1,
                ManagedMetadata.newBuilder().setCreateUser("cde").build(),
                null);
        txn.commit();
        assertThat(shimStore.getTable(someNamespace, tableName).get(key1).getMetadata())
                .isNull();
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception exception
     */
    @Test
    public void checkMetadataMergesOldFieldsTest() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, LogReplicationEntryMetadataMsg> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                LogReplicationEntryMetadataMsg.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UuidMsg key = UuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();
        final String something = "double_nested_metadata_field";
        final int one = 1; // Frankly stupid but i could not figure out how to selectively disable checkstyle
        final long twelve = 12L; // please help figure out how to disable checkstyle selectively

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    LogReplicationEntryMetadataMsg.newBuilder()
                            .setTopologyConfigID(twelve)
                            .setSyncRequestId(UuidMsg.newBuilder().setMsb(one).build())
                            .build());
            txn.commit();
        }

        // Update the record, validate that metadata fields not set, get merged with existing
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    LogReplicationEntryMetadataMsg.newBuilder()
                            .setTimestamp(one + twelve)
                            .build());
            txn.commit();
        }
        CorfuStoreEntry<UuidMsg, ManagedMetadata, LogReplicationEntryMetadataMsg> entry = null;
        try (ManagedTxnContext queryTxn = shimStore.tx(someNamespace)) {
            entry = queryTxn.getRecord(table, key);
        }

        assertThat(entry.getMetadata().getTopologyConfigID()).isEqualTo(twelve);
        assertThat(entry.getMetadata().getSyncRequestId().getMsb()).isEqualTo(one);
        assertThat(entry.getMetadata().getTimestamp()).isEqualTo(twelve + one);

        // Rolling Upgrade compatibility test: It should be ok to set a different metadata schema message
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    ManagedMetadata.newBuilder()
                            .build(), true);
            txn.commit();
        }
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataWorksWithoutSupervision() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UuidMsg key = UuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.commit();
        }

        CorfuStoreEntry<UuidMsg, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext query = shimStore.tx(someNamespace)) {
            entry = query.getRecord(tableName, key);
        }
        assertThat(entry.getMetadata().getRevision()).isEqualTo(0);
        assertThat(entry.getMetadata().getCreateTime()).isGreaterThan(0);
        assertThat(entry.getMetadata().getCreateTime()).isEqualTo(entry.getMetadata().getLastModifiedTime());

        class CommitCallbackImpl implements TxnContext.CommitCallback {

            CorfuStreamEntry.OperationType expectedOperation;

            private boolean called;

            public CommitCallbackImpl(CorfuStreamEntry.OperationType expectedOperation) {
                this.expectedOperation = expectedOperation;
            }

            public void onCommit(Map<String, List<CorfuStreamEntry>> mutations) {
                assertThat(mutations.size()).isEqualTo(1);
                assertThat(mutations.containsKey(table.getFullyQualifiedTableName())).isTrue();
                assertThat(mutations.get(table.getFullyQualifiedTableName()).size()).isEqualTo(1);
                // This one way to selectively extract the metadata out
                ManagedMetadata metadata = (ManagedMetadata) mutations.get(table.getFullyQualifiedTableName()).get(0).getMetadata();
                if (metadata != null) {
                    assertThat(metadata.getRevision()).isGreaterThan(0);
                }

                assertThat(Iterables.getOnlyElement(mutations.values()).get(0).getOperation())
                        .isEqualTo(expectedOperation);
                called = true;
            }

            public boolean isVerified() {
                return called;
            }
        }

        CommitCallbackImpl updateCb = new CommitCallbackImpl(CorfuStreamEntry.OperationType.UPDATE);

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.addCommitCallback(updateCb);
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.commit();
        }

        assertThat(updateCb.isVerified()).isTrue();

        CommitCallbackImpl deleteCb = new CommitCallbackImpl(CorfuStreamEntry.OperationType.DELETE);

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.addCommitCallback(deleteCb);
            txn.deleteRecord(tableName, key, ManagedMetadata.newBuilder().build());
            txn.commit();
        }

        assertThat(deleteCb.isVerified()).isTrue();

        CommitCallbackImpl clearCb = new CommitCallbackImpl(CorfuStreamEntry.OperationType.CLEAR);

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.addCommitCallback(clearCb);
            txn.clear(tableName);
            txn.commit();
        }

        assertThat(clearCb.isVerified()).isTrue();
    }

    /**
     * Validate that nested transactions do not throw exception if txnWithNesting is used.
     *
     * @throws Exception
     */
    @Test
    public void checkNestedTransaction() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UuidMsg key = UuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        class NestedTxnTester {
            public void nestedQuery() {
                CorfuStoreEntry<UuidMsg, ManagedMetadata, ManagedMetadata> entry;
                try (ManagedTxnContext rwTxn = shimStore.txn(someNamespace)) {
                    entry = rwTxn.getRecord(tableName, key);
                    // Nested transactions can also supply commitCallbacks that will be invoked when
                    // the transaction actually commits.
                    rwTxn.addCommitCallback((mutations) -> {
                        assertThat(mutations).containsKey(table.getFullyQualifiedTableName());
                        assertThat(TransactionalContext.isInTransaction()).isFalse();
                    });
                    rwTxn.commit();
                }
                assertThat(TransactionalContext.isInTransaction()).isTrue();
                assertThat(entry.getMetadata().getRevision()).isEqualTo(0);
                assertThat(entry.getMetadata().getCreateTime()).isGreaterThan(0);
                assertThat(entry.getMetadata().getCreateTime()).isEqualTo(entry.getMetadata().getLastModifiedTime());
            }
        }
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            NestedTxnTester nestedTxnTester = new NestedTxnTester();
            nestedTxnTester.nestedQuery();
            txn.commit();
        }

        assertThat(TransactionalContext.isInTransaction()).isFalse();

        // ----- check nested transactions NOT started by CorfuStore isn't messed up by CorfuStore txn -----
        PersistentCorfuTable<String, String>
                corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuRuntime.getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE).build().begin();
        corfuTable.insert("k1", "a"); // Load non-CorfuStore data
        corfuTable.insert("k2", "ab");
        corfuTable.insert("k3", "b");
        CorfuStoreEntry<UuidMsg, ManagedMetadata, ManagedMetadata> entry;
        try (ManagedTxnContext nestedTxn = shimStore.txn(someNamespace)) {
            nestedTxn.putRecord(tableName, key, ManagedMetadata.newBuilder().setLastModifiedUser("secondUser").build());
            entry = nestedTxn.getRecord(tableName, key);
            nestedTxn.commit(); // should not commit the parent transaction!
        }
        assertThat(entry.getMetadata().getRevision()).isGreaterThan(0);

        assertThat(TransactionalContext.isInTransaction()).isTrue();
        long commitAddress = corfuRuntime.getObjectsView().TXEnd();
        assertThat(commitAddress).isNotEqualTo(Address.NON_ADDRESS);
    }

    /**
     * This test validates that the CorfuQueue api via the CorfuStore layer binds the fate and order of the
     * queue operations with that of its parent transaction.
     *
     * @throws Exception could be a corfu runtime exception if bad things happen.
     */
    @Test
    public void queueOrderInTxnContext() throws Exception {
        final int numIterations = 1000;
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String conflictTableName = "ConflictTable";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<UuidMsg, UuidMsg, Message> conflictTable =
                shimStore.openTable(
                        someNamespace,
                        conflictTableName,
                        UuidMsg.class,
                        UuidMsg.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        Table<Queue.CorfuGuidMsg, ExampleSchemas.ExampleValue, Queue.CorfuQueueMetadataMsg> corfuQueue =
                shimStore.openQueue(someNamespace, "testQueue",
                        ExampleSchemas.ExampleValue.class,
                        TableOptions.builder().build());
        ArrayList<Long> validator = new ArrayList<>(numIterations);
        for (long i = 0L; i < numIterations; i++) {
            ExampleSchemas.ExampleValue queueData = ExampleSchemas.ExampleValue.newBuilder()
                    .setPayload("" + i)
                    .setAnotherKey(i).build();
            final int two = 2;
            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                long coinToss = new Random().nextLong() % two;
                UuidMsg conflictKey = UuidMsg.newBuilder().setMsb(coinToss).build();
                txn.putRecord(conflictTable, conflictKey, conflictKey);
                txn.enqueue(corfuQueue, queueData);
                if (coinToss > 0) {
                    final CorfuStoreMetadata.Timestamp streamOffset = txn.commit();
                    validator.add(i);
                    log.debug("ENQ: {} => {} at {}", i, queueData, streamOffset.getSequence());
                } else {
                    txn.txAbort();
                }
            }
        }
        // After all tentative transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<Table.CorfuQueueRecord> records;
        try (ManagedTxnContext query = shimStore.txn(someNamespace)) {
            records = corfuQueue.entryList();
        }
        assertThat(validator.size()).isEqualTo(records.size());

        // Also validate that the order of the queue matches that of the commit order.
        for (int i = 0; i < validator.size(); i++) {
            log.debug("Entry:" + records.get(i).getRecordId());
            Long order = ((ExampleSchemas.ExampleValue) records.get(i).getEntry()).getAnotherKey();
            assertThat(order).isEqualTo(validator.get(i));
        }
    }

    /**
     * This is a research work done to demonstrate how Google DynamicMessage can be used to print/dump
     * the contents of the protobuf store which was written by a fully qualified type.
     *
     * @throws Exception
     */
    @Ignore
    @Test
    public void dynamicMessageProtobufTest() throws Exception {
        final int ruleId = 123;
        FirewallRule rule = FirewallRule
                .newBuilder()
                .setRuleId(ruleId)
                .setRuleName("TestRule")
                .setInput(Appliance.newBuilder().setEndpoint("127.0.0.1").build())
                .setOutput(Appliance.newBuilder().setEndpoint("196.168.0.1").build())
                .build();

        Message message = rule;

        message.getAllFields().forEach((fieldDescriptor, field) -> {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                log.info("Detected version in field " + fieldDescriptor.getName() + " = " + field);
            }
        });

        FileDescriptor applianceFileDescriptor = Appliance.getDescriptor().getFile();
        FileDescriptor firewallFileDescriptor = FirewallRule.getDescriptor().getFile();
        FileDescriptor schemaMetadataFileDescriptor = CorfuOptions.getDescriptor();
        FileDescriptor googleDescriptor = DescriptorProto.getDescriptor().getFile();

        byte[] data = message.toByteArray();
        byte[] applianceSchemaBytes = applianceFileDescriptor.toProto().toByteArray();
        byte[] firewallSchemaBytes = firewallFileDescriptor.toProto().toByteArray();
        byte[] metadataSchemaBytes = schemaMetadataFileDescriptor.toProto().toByteArray();
        byte[] googleSchemaBytes = googleDescriptor.toProto().toByteArray();

        FileDescriptorProto applianceSchemaProto = FileDescriptorProto.parseFrom(applianceSchemaBytes);
        FileDescriptorProto firewallSchemaProto = FileDescriptorProto.parseFrom(firewallSchemaBytes);
        FileDescriptorProto metadataSchemaProto = FileDescriptorProto.parseFrom(metadataSchemaBytes);
        FileDescriptorProto googleDescriptorProto = FileDescriptorProto.parseFrom(googleSchemaBytes);

        FileDescriptorSet fileDescriptorSet = FileDescriptorSet.newBuilder()
                .addFile(applianceSchemaProto)
                .addFile(firewallSchemaProto)
                .addFile(metadataSchemaProto)
                .addFile(googleDescriptorProto)
                .build();

        Map<String, FileDescriptorProto> fileDescriptorProtoMap = new HashMap<>();
        fileDescriptorSet.getFileList().forEach(fileDescriptorProto -> fileDescriptorProtoMap.put(
                fileDescriptorProto.getName(), fileDescriptorProto));

        Any any = Any.pack(message);
        byte[] anyBytes = any.toByteArray();
        log.info("{}", anyBytes);
        any = Any.parseFrom(anyBytes);
        log.info(any.getTypeUrl());
        printMessage(data, fileDescriptorProtoMap);
    }

    /**
     * Verify null Value and Key schemas are blocked on openTable.
     *
     * @throws Exception
     */
    @Test
    public void checkNullValueAndKeySchema() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table
        assertThatThrownBy(() -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                null,
                null,
                TableOptions.builder().build())).isExactlyInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> shimStore.openTable(
                someNamespace,
                tableName,
                null,
                ManagedMetadata.class,
                null,
                TableOptions.builder().build())).isExactlyInstanceOf(NullPointerException.class);
    }

    /**
     * Demonstrates how features switches can be extracted from table options.
     *
     * @throws Exception
     */
    @Test
    public void checkTableOptions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, Message> table =
                shimStore.openTable(
                        someNamespace,
                        tableName,
                        SampleSchema.Uuid.class,
                        SampleSchema.SampleTableAMsg.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        String streamTag1 = SampleSchema.SampleTableAMsg.getDescriptor()
                .getOptions().getExtension(CorfuOptions.tableSchema).getStreamTag(0);
        String streamTag2 = SampleSchema.SampleTableAMsg.getDescriptor()
                .getOptions().getExtension(CorfuOptions.tableSchema).getStreamTag(1);
        assertThat(streamTag1).isEqualTo("sample_streamer_1");
        assertThat(streamTag2).isEqualTo("sample_streamer_2");

        final TableRegistry tableRegistry = corfuRuntime.getTableRegistry();
        final CorfuStoreMetadata.TableName tableNameProto = CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(table.getNamespace()).setTableName(tableName).build();

        assertThat(tableRegistry.getRegistryTable().get(tableNameProto).getMetadata().getTableOptions().getStreamTag(0)).isEqualTo(streamTag1);
        assertThat(tableRegistry.getRegistryTable().get(tableNameProto).getMetadata().getTableOptions().getStreamTag(1)).isEqualTo(streamTag2);
    }

    private void printMessage(byte[] data, Map<String, FileDescriptorProto> map) throws Exception {

        FileDescriptor firewallDescriptor = getDescriptors("sample_schema.proto", map);
        DynamicMessage msg = DynamicMessage.parseFrom(firewallDescriptor.findMessageTypeByName("FirewallRule"), data);
        log.info(msg.toString());
    }

    private FileDescriptor getDescriptors(String name, Map<String, FileDescriptorProto> map) throws DescriptorValidationException {

        List<FileDescriptor> list = new ArrayList<>();
        for (String s : map.get(name).getDependencyList()) {
            FileDescriptor descriptors = getDescriptors(s, map);
            list.add(descriptors);
        }
        FileDescriptor[] fileDescriptors = list.toArray(new FileDescriptor[list.size()]);
        return FileDescriptor.buildFrom(map.get(name), fileDescriptors);
    }

    /**
     * ProtobufDescriptorTable should de-duplicate common protobuf file descriptors.
     * This test creates a large number of tables that share protobuf files and validates
     * that de-duplication occurs.
     * <p>
     * It also verifies that the second registration is omitted.
     *
     * @throws Exception exception
     */
    @Test
    public void checkProtoDeduplication() throws Exception {
        CorfuRuntime corfuRuntime = getTestRuntime();
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);
        final String someNamespace = "some-namespace";
        final String tableNamePrefix = "prefixTable";
        final int numTables = PARAMETERS.NUM_ITERATIONS_VERY_LOW;

        Map<String, FileDescriptor> referenceMap = new HashMap<>();
        UuidMsg uuidMsg = UuidMsg.getDefaultInstance();
        ManagedMetadata managedMetadata = ManagedMetadata.getDefaultInstance();

        // Now we add all our known protobuf files involved in this test into a reference map:
        referenceMap.put(uuidMsg.getDescriptorForType().getFile().getFullName(),
                uuidMsg.getDescriptorForType().getFile());
        referenceMap.put(managedMetadata.getDescriptorForType().getFile().getFullName(),
                managedMetadata.getDescriptorForType().getFile());
        referenceMap.put(RpcCommon.getDescriptor().getFullName(),
                RpcCommon.getDescriptor().getFile());
        referenceMap.put(CorfuStoreMetadata.getDescriptor().getFullName(),
                CorfuStoreMetadata.getDescriptor().getFile());
        referenceMap.put(CorfuOptions.getDescriptor().getFullName(),
                CorfuOptions.getDescriptor().getFile());

        for (int i = 0; i < numTables; i++) {
            shimStore.openTable(
                    someNamespace,
                    tableNamePrefix + i,
                    UuidMsg.class,
                    ManagedMetadata.class,
                    ManagedMetadata.class,
                    // TableOptions includes option to choose - Memory/Disk based corfu table.
                    TableOptions.builder().build());
        }

        final PersistentCorfuTable<ProtobufFileName,
                CorfuRecord<ProtobufFileDescriptor,
                        CorfuStoreMetadata.TableMetadata>> descriptorTable =
                shimStore.getRuntime().getTableRegistry().getProtobufDescriptorTable();
        for (ProtobufFileName fileName : descriptorTable.keySet()) {
            if (fileName.getFileName().startsWith("google/protobuf")) {
                continue; // Do not validate library protos
            }
            assertThat(referenceMap.get(fileName.getFileName())).isNotNull();
        }

        final int numProtoFiles =
                corfuRuntime.getTableRegistry().getProtobufDescriptorTable().size();
        CorfuStoreMetadata.TableName tableName = CorfuStoreMetadata.TableName
                .newBuilder()
                .setNamespace(someNamespace)
                .setTableName(tableNamePrefix + "0")
                .build();

        Collection<CorfuRecord<ProtobufFileDescriptor, CorfuStoreMetadata.TableMetadata>> records =
                corfuRuntime.getTableRegistry().getProtobufDescriptorTable().entryStream().map(Map.Entry::getValue).collect(Collectors.toList());

        shimStore.openTable(
                someNamespace,
                tableNamePrefix + "0",
                UuidMsg.class,
                org.corfudb.runtime.proto.LogData.LogDataMsg.class, // this brings in 1 new protobuf file
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        referenceMap.put(org.corfudb.runtime.proto.LogData.LogDataMsg.getDescriptor().getFile().getFullName(),
                org.corfudb.runtime.proto.LogData.getDescriptor().getFile());

        // verify the second registration is omitted.
        final int numProtoFilesAfterChange =
                corfuRuntime.getTableRegistry().getProtobufDescriptorTable().size();

        final Collection<CorfuRecord<ProtobufFileDescriptor, CorfuStoreMetadata.TableMetadata>> recordsAfterChange =
                corfuRuntime.getTableRegistry().getProtobufDescriptorTable().entryStream().map(Map.Entry::getValue).collect(Collectors.toList());

        // Refresh descriptor table
        records = corfuRuntime.getTableRegistry()
                .getProtobufDescriptorTable()
                .entryStream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        // We added new protofile logdata
        assertThat(numProtoFiles).isEqualTo(numProtoFilesAfterChange - 1);
        assertThat(records).containsExactlyElementsOf(recordsAfterChange);
    }

    @Test
    public void validateCommitAlwaysReturnsRealAddress() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();
        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        final String someNamespace = "some-namespace";
        final String tableName = "ManagedMetadata";
        Table<UuidMsg, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());
        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            txnContext.count(table);
            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }
        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }
    }

    /**
     * This test verifies that registry table will be updated when a table is opened
     * again with different schema options.
     */
    @Test
    public void testRegistryTableUpdateOnRecordChange() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        CorfuStoreMetadata.TableName tableNameKey =
                CorfuStoreMetadata.TableName.newBuilder()
                        .setNamespace(someNamespace)
                        .setTableName(tableName)
                        .build();
        CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata> record
                = corfuRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
        CorfuOptions.SchemaOptions options = record.getMetadata().getTableOptions();

        assertThat(options.getIsFederated()).isTrue();
        assertThat(options.getRequiresBackupSupport()).isTrue();
        assertThat(options.getStreamTag(0)).isEqualTo("sample_streamer_1");
        assertThat(options.getStreamTag(1)).isEqualTo("sample_streamer_2");

        // Open the same table again with different schema options, verify that registry table
        // gets updated
        shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableBMsg.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        record = corfuRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
        options = record.getMetadata().getTableOptions();

        assertThat(options.getIsFederated()).isTrue();
        assertThat(options.getRequiresBackupSupport()).isFalse();
        assertThat(options.getStreamTag(0)).isEqualTo("sample_streamer_2");
        assertThat(options.getStreamTag(1)).isEqualTo("sample_streamer_3");

        // Open the same table again with different schema options, verify that registry table
        // gets updated
        shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableBMsg.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        record = corfuRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
        options = record.getMetadata().getTableOptions();

        assertThat(options.getIsFederated()).isTrue();
        assertThat(options.getRequiresBackupSupport()).isFalse();
        assertThat(options.getStreamTagCount()).isEqualTo(2);
        assertThat(options.getStreamTag(0)).isEqualTo("sample_streamer_2");
        assertThat(options.getStreamTag(1)).isEqualTo("sample_streamer_3");
        assertThat(options.getReplicationGroup().getLogicalGroup()).isEqualTo("group1");
        assertThat(options.getReplicationGroup().getClientName()).isEqualTo("logical_group_consumer");

        // Open the same table again with different schema options, verify that registry table
        // gets updated
        shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableEMsg.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        record = corfuRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
        options = record.getMetadata().getTableOptions();

        assertThat(options.getIsFederated()).isTrue();
        assertThat(options.getRequiresBackupSupport()).isFalse();
        assertThat(options.getStreamTagCount()).isEqualTo(1);
        assertThat(options.getStreamTag(0)).isEqualTo("sample_streamer_4");
        assertThat(options.getSecondaryKeyCount()).isEqualTo(1);
        assertThat(options.getSecondaryKey(0).getIndexPath()).isEqualTo("event_time");
    }

    /**
     * Ensure that {@link ObjectID}'s type is used during object cache operations.
     */
    @Test
    public void testTableType() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "DiskTable";
        final String dataPath = Files.createTempDirectory(tableName).toString();

        PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                .setDataPath(dataPath)
                .build();

        final Table<?, ?, ?> inMemoryTable = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().build());
        assertThat(inMemoryTable.getUnderlyingType()).isEqualTo(PersistentCorfuTable.class);

        final Table<?, ?, ?> diskBackedTable = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().persistenceOptions(persistenceOptions).build());
        assertThat(diskBackedTable.getUnderlyingType()).isEqualTo(PersistedCorfuTable.class);

        inMemoryTable.close();
        diskBackedTable.close();
    }

    @Test
    public void testCloseOpen() throws Exception {
        final int numEntries = 1000;
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "DiskTable";
        final String dataPath = Files.createTempDirectory(tableName).toString();
        final String LOCK_FILE = "LOCK";

        PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                .setDataPath(dataPath)
                .build();

        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, ManagedResources> table = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().persistenceOptions(persistenceOptions).build());


        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            for (int i = 0; i < numEntries; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                txnContext.putRecord(table, key, value, null);
            }

            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }

        assertTrue(Files.exists(Paths.get(dataPath, LOCK_FILE)));
        table.close();
        assertFalse(Files.exists(Paths.get(dataPath, LOCK_FILE)));

        table = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().persistenceOptions(persistenceOptions).build());

        assertThat(table.count()).isEqualTo(numEntries);
    }

    private Cache getBlockCache(Options options) throws NoSuchFieldException, IllegalAccessException {
        final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        final Field field = BlockBasedTableConfig.class.getDeclaredField("blockCache");
        field.setAccessible(true);
        return (Cache) field.get(tableConfig);
    }

    private Options getRocksDbOptions(Table<?, ?, ?> table) {
        final ObjectID thisId = ObjectID.builder()
                .type( PersistedCorfuTable.getTypeToken().getRawType())
                .streamID(table.getStreamUUID()).build();
        final PersistedCorfuTable<?, ?> persistedTable = (PersistedCorfuTable<?, ?>) getRuntime().getObjectsView()
                .getObjectCache().get(thisId);
        final DiskBackedCorfuTable<?, ?> diskTable = (DiskBackedCorfuTable<?, ?>) persistedTable
                .getCorfuSMRProxy().getUnderlyingMVO().getCurrentObject();
        final RocksDbStore<?> rocksStore = (RocksDbStore<?>) diskTable.getRocksApi();
        return rocksStore.getRocksDbOptions();
    }

    @Test
    public void testBlockCache() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        final String tableName = "DiskTable";

        {
            final String dataPath = Files.createTempDirectory(tableName).toString();
            final int INVALID_CACHE_INDEX = 100;
            PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                    .setDataPath(dataPath)
                    .setBlockCacheIndex(INVALID_CACHE_INDEX)
                    .build();

            assertThatThrownBy(() -> shimStore.openTable(someNamespace, tableName,
                            SampleSchema.Uuid.class,
                            SampleSchema.SampleTableAMsg.class,
                            ManagedResources.class,
                            TableOptions.builder().persistenceOptions(persistenceOptions).build()))
                    .isInstanceOf(NoSuchElementException.class);

            // Provide invalid indexes.
            disposeBlockCache(INVALID_CACHE_INDEX);
            disposeBlockCache(Integer.MAX_VALUE);
            disposeBlockCache(Integer.MIN_VALUE);
        }

        {
            final int writeBufferSize = 4 * 1024 * 1024; // 2MB
            final int blockCacheSize = 4 * 1024 * 1024; // 2MB
            final int blockCacheIndex = newBlockCache(blockCacheSize);
            final String dataPath = Files.createTempDirectory(tableName).toString();
            PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                    .setDataPath(dataPath)
                    .setWriteBufferSize(writeBufferSize)
                    .setBlockCacheIndex(blockCacheIndex)
                    .build();

            try (Table<SampleSchema.SampleTableAMsg, SampleSchema.SampleTableAMsg, ManagedResources> table =
                         shimStore.openTable(someNamespace, tableName,
                                 SampleSchema.SampleTableAMsg.class,
                                 SampleSchema.SampleTableAMsg.class,
                                 ManagedResources.class,
                                 TableOptions.builder().persistenceOptions(persistenceOptions).build())) {
                final Options rocksDbOptions = getRocksDbOptions(table);
                assertThat(rocksDbOptions.writeBufferSize()).isEqualTo(writeBufferSize);
                final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) rocksDbOptions.tableFormatConfig();
                assertThat(tableConfig.noBlockCache()).isEqualTo(false);
                final Cache cache = getBlockCache(rocksDbOptions);
                assertThat(org.corfudb.runtime.object.PersistenceOptions.getBlockCache(blockCacheIndex)).isEqualTo(cache);

                disposeBlockCache(blockCacheIndex);
                // Ensure that the action is idempotent.
                disposeBlockCache(blockCacheIndex);
            }
        }

        {
            final int writeBufferSize = 4 * 1024 * 1024; // 4MB
            final String dataPath = Files.createTempDirectory(tableName).toString();
            PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                    .setDataPath(dataPath)
                    .setWriteBufferSize(writeBufferSize)
                    .setBlockCacheIndex(DISABLE_BLOCK_CACHE)
                    .build();

            try (Table<SampleSchema.SampleTableAMsg, SampleSchema.SampleTableAMsg, ManagedResources> table =
                         shimStore.openTable(someNamespace, tableName,
                                 SampleSchema.SampleTableAMsg.class,
                                 SampleSchema.SampleTableAMsg.class,
                                 ManagedResources.class,
                                 TableOptions.builder().persistenceOptions(persistenceOptions).build())) {

                final Options rocksDbOptions = getRocksDbOptions(table);
                assertThat(rocksDbOptions.writeBufferSize()).isEqualTo(writeBufferSize);
                final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) rocksDbOptions.tableFormatConfig();
                assertThat(tableConfig.noBlockCache()).isEqualTo(true);
            }
        }
    }

    @Test
    public void testDiskBackedTable() throws Exception {
        final int numEntries = 1000;
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "DiskTable";
        final String dataPath = Files.createTempDirectory(tableName).toString();
        final String LOCK_FILE = "LOCK";

        PersistenceOptions persistenceOptions = PersistenceOptions.newBuilder()
                .setDataPath(dataPath)
                .build();

        final Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, ManagedResources> table = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().persistenceOptions(persistenceOptions).build());

        assertTrue(Files.exists(Paths.get(dataPath, LOCK_FILE)));

        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            for (int i = 0; i < numEntries; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                txnContext.putRecord(table, key, value, null);
            }

            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }

        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            // Ensure that count() works.
            assertThat(txnContext.count(table)).isEqualTo(numEntries);

            final Set<SampleSchema.Uuid> keySet = new HashSet<>();
            final Set<SampleSchema.SampleTableAMsg> valueSet = new HashSet<>();


            // Ensure that get() works.
            for (int i = 0; i < numEntries; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleSchema.SampleTableAMsg expectedKey = SampleSchema.SampleTableAMsg.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                CorfuStoreEntry<?, SampleSchema.SampleTableAMsg, ?> entry = txnContext.getRecord(table, key);
                assertThat(entry.getPayload()).isEqualTo(expectedKey);
                keySet.add(key);
                valueSet.add(expectedKey);
            }

            // Check the streaming API.
            assertThat(txnContext.entryStream(table)
                            .map(CorfuStoreEntry::getKey).collect(Collectors.toSet()))
                    .isEqualTo(keySet);

            assertThat(txnContext.entryStream(table)
                    .map(CorfuStoreEntry::getPayload).collect(Collectors.toSet()))
                    .isEqualTo(valueSet);

            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }

        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            for (int i = 0; i < numEntries/2; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder()
                        .setPayload(String.valueOf(i)).build();
                txnContext.delete(table, key);
            }

            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }

        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            // Make sure the entries were actually removed.
            assertThat(txnContext.count(table)).isEqualTo(numEntries/2);

            CorfuStoreMetadata.Timestamp ts = txnContext.commit();
            assertThat(ts.getSequence()).isPositive();
        }

        // Ensure that the underlying RocksDB instance is released.
        try (ManagedTxnContext ignored = shimStore.tx(someNamespace)) {
            shimStore.freeTableData(someNamespace, tableName);
        }

        final Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, ManagedResources> newTable = shimStore.openTable(
                someNamespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                ManagedResources.class,
                TableOptions.builder().persistenceOptions(persistenceOptions).build());

        try (ManagedTxnContext txnContext = shimStore.tx(someNamespace)) {
            assertThat(txnContext.count(table)).isEqualTo(numEntries/2);
        }
    }
}
