package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.test.SampleSchema;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static com.google.protobuf.Descriptors.FileDescriptor;
import static org.corfudb.test.SampleAppliance.Appliance;
import static org.corfudb.test.SampleSchema.FirewallRule;
import static org.corfudb.test.SampleSchema.ManagedResources;

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
            assertThatThrownBy( () -> readWriteTxn.putRecord(tableName, key2, null, null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }
        log.debug(table.getMetrics().toString());
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
        CorfuStoreShim corfuStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, ManagedResources> table = corfuStore.openTable(
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

        try (ManagedTxnContext txn = corfuStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("abc").build(), user_1);
            txn.commit();
        }

        assertThat(corfuStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_1")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Set the version field to the correct value 1 and expect that no exception is thrown
        try (ManagedTxnContext txn = corfuStore.tx(nsxManager)) {
            // Enforce version number
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("bcd").build(),
                    ManagedResources.newBuilder().setCreateUser("user_2").setVersion(0L).build());
            txn.commit();
        }

        // Honor enforced version number
        assertThat(corfuStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(0L).build());

        // Now do an update without setting the version field, and it should not get validated!
        try (ManagedTxnContext txn = corfuStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("cde").build(),
                    user_2);
            txn.commit();
        }

        assertThat(corfuStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        try (ManagedTxnContext txn = corfuStore.tx(nsxManager)) {
            txn.delete(table, key1);
            txn.commit();
        }
        assertThat(corfuStore.getTable(nsxManager, tableName).get(key1)).isNull();
        expectedVersion = 0L;

        try (ManagedTxnContext txn = corfuStore.tx(nsxManager)) {
            txn.putRecord(table, key1, SampleSchema.EventInfo.newBuilder().setName("def").build(), user_2);
            txn.commit();
        }

        assertThat(corfuStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder(user_2)
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Validate that if Metadata schema is specified, a null metadata is not acceptable
        assertThatThrownBy(() ->
                table.update(key1, SampleSchema.EventInfo.getDefaultInstance(), null))
                .isExactlyInstanceOf(RuntimeException.class);

        // Validate that we throw a StaleObject exception if there is an explicit version mismatch
        ManagedResources wrongRevisionMetadata = ManagedResources.newBuilder(user_2)
                .setVersion(2).build();

        assertThatThrownBy(() ->
                table.update(key1, SampleSchema.EventInfo.getDefaultInstance(), wrongRevisionMetadata))
                .isExactlyInstanceOf(RuntimeException.class);

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
        CorfuStoreShim corfuStore = new CorfuStoreShim(corfuRuntime);
        final String nsxManager = "nsx-manager"; // namespace for the table
        final String tableName = "EventInfo"; // table name
        final int numThreads = 5;
        scheduleConcurrently(numThreads, t -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                // Create & Register the table.
                // This is required to initialize the table for the current corfu client.
                corfuStore.openTable(nsxManager, tableName, SampleSchema.Uuid.class, SampleSchema.EventInfo.class, null,
                        TableOptions.builder().build());
            }

        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);
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
        txn1.putRecord(tableName, key1,
                ManagedMetadata.newBuilder().setCreateUser("abc").build(),
                ManagedMetadata.newBuilder().setRevision(1L).build());
        assertThatThrownBy(txn1::commit).isExactlyInstanceOf(StaleRevisionUpdateException.class);

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
        log.debug(table.getMetrics().toString());
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
        log.debug(table.getMetrics().toString());
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
                            .setTimestamp(one+twelve)
                            .build());
            txn.commit();
        }
        CorfuStoreEntry<UuidMsg, ManagedMetadata, LogReplicationEntryMetadataMsg> entry = null;
        try (ManagedTxnContext queryTxn = shimStore.tx(someNamespace)) {
            entry = queryTxn.getRecord(table, key);
        }

        assertThat(entry.getMetadata().getTopologyConfigID()).isEqualTo(twelve);
        assertThat(entry.getMetadata().getSyncRequestId().getMsb()).isEqualTo(one);
        assertThat(entry.getMetadata().getTimestamp()).isEqualTo(twelve+one);

        // Rolling Upgrade compatibility test: It should be ok to set a different metadata schema message
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    ManagedMetadata.newBuilder()
                            .build(), true);
            txn.commit();
        }
        log.debug(table.getMetrics().toString());
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
            public void onCommit(Map<String, List<CorfuStreamEntry>> mutations) {
                assertThat(mutations.size()).isEqualTo(1);
                assertThat(mutations.get(table.getFullyQualifiedTableName()).size()).isEqualTo(1);
                // This one way to selectively extract the metadata out
                ManagedMetadata metadata = (ManagedMetadata) mutations.get(table.getFullyQualifiedTableName()).get(0).getMetadata();
                assertThat(metadata.getRevision()).isGreaterThan(0);

                // This is another way to extract the metadata out..
                mutations.forEach((tblName, entries) -> {
                    entries.forEach(mutation -> {
                        // This is how we can extract the metadata out
                        ManagedMetadata metaData = (ManagedMetadata) mutations.get(tblName).get(0).getMetadata();
                        assertThat(metaData.getRevision()).isGreaterThan(0);
                    });
                });
            }
        }

        CommitCallbackImpl commitCallback = new CommitCallbackImpl();
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
                });
            });
            txn.commit();
        }

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.deleteRecord(tableName, key, ManagedMetadata.newBuilder().build());
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.DELETE);
                });
            });
            txn.commit();
        }

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.clear(tableName);
            txn.addCommitCallback((mutations) -> {
                mutations.values().forEach(mutation -> {
                    CorfuStreamEntry.OperationType op = mutation.get(0).getOperation();
                    assertThat(op).isEqualTo(CorfuStreamEntry.OperationType.CLEAR);
                });
            });
            txn.commit();
        }
        log.debug(table.getMetrics().toString());
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
        CorfuTable<String, String>
                corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        corfuRuntime.getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE).build().begin();
        corfuTable.put("k1", "a"); // Load non-CorfuStore data
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");
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
        log.debug(table.getMetrics().toString());
    }

    /**
     * This test validates that the CorfuQueue api via the CorfuStore layer binds the fate and order of the
     * queue operations with that of its parent transaction.
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
                    .setPayload(""+i)
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
            Long order = ((ExampleSchemas.ExampleValue)records.get(i).getEntry()).getAnotherKey();
            assertThat(order).isEqualTo(validator.get(i));
        }
        log.debug(corfuQueue.getMetrics().toString());
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
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getSecondaryKey()) {
                log.info("Detected secondary key " + fieldDescriptor.getName() + " = " + field);
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
        assertThatThrownBy( () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                null,
                null,
                TableOptions.builder().build())).isExactlyInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy( () -> shimStore.openTable(
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
        CorfuStoreShim corfuStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, Message> table =
                corfuStore.openTable(
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

        assertThat(tableRegistry.getRegistryTable().get(tableNameProto).getMetadata().getTableOptions().getOwnershipValidation()).isTrue();
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
}
