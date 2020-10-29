package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.ManagedMetadata;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * To ensure that feature changes in CorfuStore do not break verticals,
 * we simulate their usage pattern with implementation and tests.
 * <p>
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class CorfuStoreShimTest extends AbstractViewTest {
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
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim ufoStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedMetadata> table = ufoStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        user_1)
                .commit();
        long tail1 = ufoStore.getHighestSequence(someNamespace, tableName);

        // Take a snapshot to test snapshot isolation transaction
        final CorfuStoreMetadata.Timestamp timestamp = ufoStore.getTimestamp();
        CorfuStoreEntry<Uuid, EventInfo, ManagedMetadata> entry;
        // Start a dirty read transaction
        try (TxnContextShim readWriteTxn = ufoStore.txn(someNamespace)) {
            readWriteTxn.putRecord(table, key1,
                    EventInfo.newBuilder().setName("xyz").build(),
                    ManagedMetadata.newBuilder().build());

            // Now within the same txn query the object and validate that it shows the local update.
            entry = readWriteTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getName()).isEqualTo("xyz");
            readWriteTxn.commit();
        }

        long tail2 = ufoStore.getHighestSequence(someNamespace, tableName);
        assertThat(tail2).isGreaterThan(tail1);

        // Try a read followed by write in same txn
        // Start a dirty read transaction
        try (TxnContextShim readWriteTxn = ufoStore.txn(someNamespace)) {
            entry = readWriteTxn.getRecord(table, key1);
            readWriteTxn.putRecord(table, key1,
                    EventInfo.newBuilder()
                            .setName("abc" + entry.getPayload().getName())
                            .build(),
                    ManagedMetadata.newBuilder().build());
            readWriteTxn.commit();
        }

        // Try a read on an older timestamp
        try (TxnContextShim readTxn = ufoStore.txn(someNamespace, IsolationLevel.snapshot(timestamp))) {
            entry = readTxn.getRecord(table, key1);
            assertThat(entry.getPayload().getName()).isEqualTo("abc");
        }
        log.debug(table.getMetrics().toString());
    }

    /**
     * Simple example to see how secondary indexes work. Please see sample_schema.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testSecondaryIndexes() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim ufoStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedMetadata> table = ufoStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        final long eventTime = 123L;

        ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder()
                                .setName("abc")
                                .setEventTime(eventTime).build(),
                        user_1)
                .commit();

        try (TxnContextShim readWriteTxn = ufoStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, EventInfo, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "event_time", eventTime);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getName()).isEqualTo("abc");
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim ufoStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedMetadata> table = ufoStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        user_1)
                .commit();

        // Validate that touch() does not change the revision
        ufoStore.txn(someNamespace)
                .touch(tableName, key1)
                .commit();

        CorfuStoreEntry<Uuid, EventInfo, ManagedMetadata> entry;
        try (TxnContextShim queryTxn = ufoStore.txn(someNamespace)) {
            entry = queryTxn.getRecord(table, key1);
        }
        assertNotNull(entry);
        assertThat(entry.getMetadata().getRevision()).isEqualTo(0L);
        assertThat(entry.getMetadata().getCreateTime()).isLessThan(System.currentTimeMillis());

        // Ensure that if metadata's revision field is set, it is validated and exception thrown if stale
        assertThatThrownBy(() -> ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        ManagedMetadata.newBuilder().setRevision(1L).build())
                .commit()).isExactlyInstanceOf(StaleRevisionUpdateException.class);

        // Correct revision field set should NOT throw an exception
        ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder().setName("xyz").build(),
                        ManagedMetadata.newBuilder().setRevision(0L).build())
                .commit();

        // Revision field not set should also not throw an exception, just internally bump up revision
        ufoStore.txn(someNamespace)
                .putRecord(tableName, key1,
                        EventInfo.newBuilder().setName("xyz").build(),
                        ManagedMetadata.newBuilder().build())
                .commit();

        try (TxnContextShim queryTxn = ufoStore.txn(someNamespace)) {
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
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim ufoStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedMetadata> table =
                ufoStore.openTable(
                        someNamespace,
                        tableName,
                        Uuid.class,
                        EventInfo.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
        ufoStore.txn(someNamespace)
                .putRecord(tableName,
                        key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        null)
                .commit();
        ufoStore.txn(someNamespace)
                .putRecord(table,
                        key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        null)
                .commit();
        Message metadata = ufoStore.getTable(someNamespace, tableName).get(key1).getMetadata();
        assertThat(metadata).isNull();

        // TODO: Finalize behavior of the following api with consumers..
        // Setting metadata into a schema that did not have metadata specified?
        ufoStore.txn(someNamespace)
                .putRecord(tableName,
                        key1,
                        EventInfo.newBuilder().setName("bcd").build(),
                        ManagedMetadata.newBuilder().setCreateUser("testUser").setRevision(1L).build())
                .commit();
        assertThat(ufoStore.getTable(someNamespace, tableName).get(key1).getMetadata())
                .isNotNull();

        // Now setting back null into the metadata which had non-null value
        ufoStore.txn(someNamespace)
                .putRecord(tableName,
                        key1,
                        EventInfo.newBuilder().setName("cde").build(),
                        null)
                .commit();
        assertThat(ufoStore.getTable(someNamespace, tableName).get(key1).getMetadata())
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
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim ufoStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, SampleSchema.ManagedResources> table = ufoStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                EventInfo.class,
                SampleSchema.ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        EventInfo value = EventInfo.newBuilder().setName("simpleValue").build();
        final String something = "double_nested_metadata_field";

        ufoStore.txn(someNamespace)
                .putRecord(tableName, key, value,
                        SampleSchema.ManagedResources.newBuilder()
                                .setCreateUser("CreateUser")
                                .setNestedType(
                                        SampleSchema.NestedTypeA.newBuilder()
                                                .addTag(
                                                        SampleSchema.NestedTypeB.newBuilder()
                                                                .setSomething(something).build()
                                                ).build()
                                ).build())
                .commit();

        final long arbitraryRevision = 12L;
        // Update the record, validate that metadata fields not set, get merged with existing
        ufoStore.txn(someNamespace)
                .putRecord(tableName, key, value,
                        SampleSchema.ManagedResources.newBuilder()
                                .setCreateUser("CreateUser")
                                .setVersion(arbitraryRevision)
                                .build())
                .commit();
        CorfuStoreEntry<Uuid, EventInfo, SampleSchema.ManagedResources> entry = null;
        try (TxnContextShim queryTxn = ufoStore.txn(someNamespace)) {
            entry = queryTxn.getRecord(table, key);
        }

        assertThat(entry.getMetadata().getVersion()).isEqualTo(arbitraryRevision);
        assertThat(entry.getMetadata().getCreateUser()).isEqualTo("CreateUser");
        assertThat(entry.getMetadata().getNestedType().getTag(0).getSomething()).isEqualTo(something);

        // Rolling Upgrade compatibility test: It should be ok to set a different metadata schema message
        ufoStore.txn(someNamespace)
                .putRecord(tableName, key, value,
                        ManagedMetadata.newBuilder()
                                .build(), true)
                .commit();

        log.debug(table.getMetrics().toString());
    }
}
