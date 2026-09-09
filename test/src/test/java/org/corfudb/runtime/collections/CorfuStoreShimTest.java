package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
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
import org.corfudb.runtime.view.ObjectsView;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicReference;
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
     * Additionally, it verifies that the exception is not thrown if the key is not touched, and
     * that suppressing the touch's stream notification does not weaken conflict detection.
     * @throws Exception
     */
    @Test
    public void testTouchReqdForReadTxWriteTxConflict() throws Exception {
        final int numIterations = 100;
        Assert.assertFalse(runReadAndWriteTxConcurrently(
                TxnContext.TouchOption.GENERATE_STREAM_NOTIFICATION, numIterations));
        Assert.assertFalse(runReadAndWriteTxConcurrently(
                TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION, numIterations));
        Assert.assertTrue(runReadAndWriteTxConcurrently(null, numIterations));
    }

    /**
     * @param touchOption if null, TX1 does not touch() the conflict key at all. Otherwise the
     *                    conflict key is touched using the given option.
     */
    private boolean runReadAndWriteTxConcurrently(TxnContext.TouchOption touchOption,
                                                  int numIterations) throws Exception {
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
        // conflict key from table1, touches it(touchOption != null) and writes it to table2.  TX2 updates the
        // conflict key in table1.
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
                    if (touchOption != null) {
                        txnContext.touch(tableName1, conflictKey, touchOption);
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
     * The stream tags accumulated so far by the transaction running on this thread. These are the
     * tags that the commit will append the transaction's log entry to, and hence the tags whose
     * subscribers get notified.
     */
    private Set<UUID> writeSetStreamTags() {
        return TransactionalContext.getCurrentContext().getWriteSetInfo().getStreamTags();
    }

    /** The SMR updates (i.e. the actual payload) queued so far for the given table. */
    private List<SMREntry> writeSetSmrUpdates(Table<?, ?, ?> table) {
        return TransactionalContext.getCurrentContext().getWriteSetInfo()
                .getWriteSet().getSMRUpdates(table.getStreamUUID());
    }

    /**
     * The conflict hashes the sequencer will resolve this transaction against, for one table.
     * Rendered as strings because the underlying Set<byte[]> compares by array identity.
     */
    private Set<String> writeSetConflictHashes(Table<?, ?, ?> table) {
        return TransactionalContext.getCurrentContext().getWriteSetInfo()
                .getHashedConflictSet()
                .getOrDefault(table.getStreamUUID(), Collections.emptySet())
                .stream()
                .map(Arrays::toString)
                .collect(Collectors.toSet());
    }

    /**
     * A suppressed touch() must still reach the sequencer for conflict resolution even when it is
     * the transaction's only operation. The commit path treats an empty write set as a read-only
     * transaction and short-circuits without contacting the sequencer, so the touch registers its
     * stream with an empty update list to stay on the write path. Without that, commit() would
     * return the snapshot address and the touch would silently resolve nothing.
     *
     * @throws Exception
     */
    @Test
    public void testTouchOnlyTransactionStillCommitsToSequencer() throws Exception {
        final String namespace = "test_namespace";
        final String tableName = "SampleTableA";

        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table =
                corfuStore.openTable(namespace, tableName,
                        SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                        SampleSchema.Uuid.class,
                        TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));

        SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        long putAddress;
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, key,
                    SampleSchema.SampleTableAMsg.newBuilder().setPayload("payload").build(), key);
            putAddress = txn.commit().getSequence();
        }

        // A transaction whose only operation is a suppressed touch must still be sequenced,
        // i.e. it must advance the log beyond the write that preceded it.
        long touchAddress;
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, key, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            // The write set carries the stream with no updates: enough to stay on the write path.
            assertThat(writeSetSmrUpdates(table)).isEmpty();
            touchAddress = txn.commit().getSequence();
        }

        assertThat(touchAddress).isGreaterThan(putAddress);
    }

    /**
     * A suppressed touch() writes no payload, but it must still bump the touched stream's tail so
     * that another transaction holding a coarse, whole-stream conflict on that table is still
     * resolved against it.
     * <p>
     * clear() is what produces such a conflict: it logs its update with a null conflict field
     * (PersistentCorfuTable.clear), which the sequencer treats as a conflict against every update
     * on the stream, resolved via its stream tail rather than a per-key hash. That tail is only
     * advanced for streams present in the token request, which is why the payload-free touch
     * registers its stream with an empty update list. Remove that registration and this test fails.
     *
     * @throws Exception
     */
    @Test
    public void testTouchConflictsWithWholeStreamConflict() throws Exception {
        final String namespace = "test_namespace";
        final String tableName = "SampleTableA";

        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table =
                corfuStore.openTable(namespace, tableName,
                        SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                        SampleSchema.Uuid.class,
                        TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));

        SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, key,
                    SampleSchema.SampleTableAMsg.newBuilder().setPayload("payload").build(), key);
            txn.commit();
        }

        // TX1 takes a whole-stream conflict on the table and stays open on this thread.
        TxnContext clearTxn = corfuStore.txn(namespace);
        try {
            clearTxn.clear(table);

            // TX2 touches a key in that table on another thread, writing nothing, and commits.
            // A separate thread is required because transactional contexts are thread-local and
            // TxnContext forbids nesting.
            AtomicReference<Throwable> touchFailure = new AtomicReference<>();
            Thread toucher = new Thread(() -> {
                try (TxnContext touchTxn = corfuStore.txn(namespace)) {
                    touchTxn.touch(table, key, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
                    touchTxn.commit();
                } catch (Throwable t) {
                    touchFailure.set(t);
                }
            });
            toucher.start();
            toucher.join();
            assertThat(touchFailure.get()).isNull();

            // TX1 must now abort: the payload-free touch still advanced the stream tail.
            assertThatThrownBy(clearTxn::commit)
                    .isInstanceOf(TransactionAbortedException.class);
        } finally {
            clearTxn.close();
        }
    }

    /**
     * The empty per-stream entry a suppressed touch() appends must survive a serialization round
     * trip and replay as a no-op, leaving the record intact for a runtime that reads the log from
     * scratch.
     *
     * @throws Exception
     */
    @Test
    public void testTouchRoundTripsAsNoOp() throws Exception {
        final String namespace = "test_namespace";
        final String tableName = "SampleTableA";

        CorfuRuntime writerRuntime = getDefaultRuntime();
        CorfuStore corfuStore = new CorfuStore(writerRuntime);
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table =
                corfuStore.openTable(namespace, tableName,
                        SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                        SampleSchema.Uuid.class,
                        TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));

        SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(7).setMsb(7).build();
        SampleSchema.SampleTableAMsg value =
                SampleSchema.SampleTableAMsg.newBuilder().setPayload("payload").build();
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, key, value, key);
            txn.commit();
        }
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, key, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            txn.commit();
        }

        // Replay the log from scratch on a separate runtime.
        CorfuRuntime readerRuntime = getNewRuntime(getDefaultNode()).connect();
        try {
            CorfuStore readerStore = new CorfuStore(readerRuntime);
            Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> readerTable =
                    readerStore.openTable(namespace, tableName,
                            SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                            SampleSchema.Uuid.class,
                            TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));
            try (TxnContext txn = readerStore.txn(namespace)) {
                CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>
                        entry = txn.getRecord(readerTable, key);
                assertThat(entry.getPayload()).isEqualTo(value);
                assertThat(entry.getMetadata()).isEqualTo(key);
                assertThat(txn.count(readerTable)).isEqualTo(1);
                txn.commit();
            }
        } finally {
            readerRuntime.shutdown();
        }
    }

    /**
     * touch() re-writes a record with its own unchanged payload, which by default makes the
     * transaction land on the table's stream tags and wake up their subscribers.
     * Verify that TouchOption.SUPPRESS_STREAM_NOTIFICATION contributes no stream tags at all,
     * that the default option still contributes all of them, and that the record is unchanged
     * either way.
     *
     * @throws Exception
     */
    @Test
    public void testTouchStreamTagSuppression() throws Exception {
        final String namespace = "test_namespace";
        final String tableName = "SampleTableA";

        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table =
                corfuStore.openTable(namespace, tableName,
                        SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                        SampleSchema.Uuid.class,
                        TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));

        // SampleTableAMsg declares sample_streamer_1 and sample_streamer_2 and is_federated,
        // so it also carries the Log Replication tag. Assert the fixture to keep the test honest.
        assertThat(table.getStreamTags()).hasSize(3)
                .contains(ObjectsView.getLogReplicatorStreamId());

        SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder()
                .setPayload("payload").build();
        // Capture the conflict hashes a real write on this key registers, to compare against.
        Set<String> putConflictHashes;
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, key, value, key);
            putConflictHashes = writeSetConflictHashes(table);
            txn.commit();
        }
        assertThat(putConflictHashes).hasSize(1);

        // A suppressed touch() contributes no stream tags, so no tag subscriber is notified, and
        // writes no payload at all - just the same conflict hash a real write would have produced,
        // which is what makes it indistinguishable to the sequencer's conflict resolution.
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, key, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            assertThat(writeSetStreamTags()).isEmpty();
            assertThat(writeSetSmrUpdates(table)).isEmpty();
            assertThat(writeSetConflictHashes(table)).isEqualTo(putConflictHashes);
            txn.commit();
        }

        // The default touch() still contributes all of the table's tags, and still writes a payload.
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, key);
            assertThat(writeSetStreamTags())
                    .containsExactlyInAnyOrderElementsOf(table.getStreamTags());
            assertThat(writeSetSmrUpdates(table)).hasSize(1);
            txn.commit();
        }

        // Neither touch() changed the record or the table size.
        try (TxnContext txn = corfuStore.txn(namespace)) {
            CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>
                    entry = txn.getRecord(table, key);
            assertThat(entry.getPayload()).isEqualTo(value);
            assertThat(entry.getMetadata()).isEqualTo(key);
            assertThat(txn.count(table)).isEqualTo(1);
            txn.commit();
        }
    }

    /**
     * Stream tags are a per-transaction union, so a suppressed touch() must not stop any other
     * mutation in the same transaction from contributing its own tags. Verify this for a
     * putRecord() in either order relative to the touch(), and for an enqueue() on a separate
     * tagged table.
     *
     * @throws Exception
     */
    @Test
    public void testTouchSuppressionKeepsTagsOfOtherMutations() throws Exception {
        final String namespace = "test_namespace";
        final String tableName = "SampleTableA";
        final String queueName = "SampleQueue";

        CorfuStore corfuStore = new CorfuStore(getDefaultRuntime());
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid> table =
                corfuStore.openTable(namespace, tableName,
                        SampleSchema.Uuid.class, SampleSchema.SampleTableAMsg.class,
                        SampleSchema.Uuid.class,
                        TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));

        SampleSchema.Uuid touchedKey = SampleSchema.Uuid.newBuilder().setLsb(0).setMsb(0).build();
        SampleSchema.Uuid otherKey = SampleSchema.Uuid.newBuilder().setLsb(1).setMsb(1).build();
        SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder()
                .setPayload("payload").build();
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, touchedKey, value, touchedKey);
            txn.commit();
        }

        // Suppressed touch() before a real putRecord(): the putRecord() brings all the tags in.
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, touchedKey, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            txn.putRecord(table, otherKey, value, otherKey);
            assertThat(writeSetStreamTags())
                    .containsExactlyInAnyOrderElementsOf(table.getStreamTags());
            txn.commit();
        }

        // Same, with the two operations in the opposite order.
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.putRecord(table, otherKey, value, otherKey);
            txn.touch(table, touchedKey, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            assertThat(writeSetStreamTags())
                    .containsExactlyInAnyOrderElementsOf(table.getStreamTags());
            txn.commit();
        }

        // An enqueue() on a differently tagged table keeps its own tags, and the suppressed
        // touch() still contributes none of the touched table's own tags.
        Table<Queue.CorfuGuidMsg, SampleSchema.ValueFieldTagOne, Queue.CorfuQueueMetadataMsg> queue =
                corfuStore.openQueue(namespace, queueName, SampleSchema.ValueFieldTagOne.class,
                        TableOptions.fromProtoSchema(SampleSchema.ValueFieldTagOne.class));
        try (TxnContext txn = corfuStore.txn(namespace)) {
            txn.touch(table, touchedKey, TxnContext.TouchOption.SUPPRESS_STREAM_NOTIFICATION);
            txn.enqueue(queue, SampleSchema.ValueFieldTagOne.newBuilder()
                    .setPayload("queued").build());
            assertThat(writeSetStreamTags())
                    .containsExactlyInAnyOrderElementsOf(queue.getStreamTags())
                    .doesNotContain(
                            TableRegistry.getStreamIdForStreamTag(namespace, "sample_streamer_1"),
                            TableRegistry.getStreamIdForStreamTag(namespace, "sample_streamer_2"));
            txn.commit();
        }
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
                TableOptions.builder().build())).isExactlyInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> shimStore.openTable(
                someNamespace,
                tableName,
                null,
                ManagedMetadata.class,
                null,
                TableOptions.builder().build())).isExactlyInstanceOf(IllegalArgumentException.class);
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
