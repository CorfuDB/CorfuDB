package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static com.google.protobuf.Descriptors.FileDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.test.SampleAppliance.Appliance;
import static org.corfudb.test.SampleSchema.FirewallRule;
import static org.corfudb.test.SampleSchema.ManagedResources;

/**
 * CorfuStoreTest demonstrates how to use the Protobuf way of accessing CorfuDB
 *
 * It comprises of tests that use the protobufs (table schemas) defined inside the proto folder.
 *
 * Created by zlokhandwala on 2019-08-12.
 */
@Slf4j
public class CorfuStoreTest extends AbstractViewTest {
    @Test
    public void basicTest() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());


        final int count = 100;
        List<Uuid> uuids = new ArrayList<>();
        List<EventInfo> events = new ArrayList<>();
        ManagedResources metadata = ManagedResources.newBuilder().setCreateUser("MrProto").build();

        // Simple CRUD using the table instance.
        // These are wrapped as transactional operations.
        table.create(Uuid.newBuilder().setLsb(0L).setMsb(0L).build(),
                EventInfo.newBuilder().setName("simpleCRUD").build(),
                metadata);

        // Fetch timestamp to perform snapshot queries or transactions at a particular timestamp.
        Timestamp timestamp = corfuStore.getTimestamp();

        // Creating a transaction builder.
        TxBuilder tx = corfuStore.tx(nsxManager);
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.nameUUIDFromBytes(Integer.toString(i).getBytes());
            Uuid uuidMsg = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits())
                    .setLsb(uuid.getLeastSignificantBits())
                    .build();
            uuids.add(uuidMsg);

            events.add(EventInfo.newBuilder()
                    .setId(i)
                    .setName("event_" + i)
                    .setEventTime(i)
                    .build());

            tx.update(tableName, uuids.get(i), events.get(i), metadata);
        }
        tx.commit();

        // Query interface.
        Query q = corfuStore.query(nsxManager);

        // Point lookup.
        final int fifty = 50;
        UUID uuid = UUID.nameUUIDFromBytes(Integer.toString(fifty).getBytes());
        Uuid lookupKey = Uuid.newBuilder()
                .setMsb(uuid.getMostSignificantBits())
                .setLsb(uuid.getLeastSignificantBits())
                .build();

        EventInfo expectedValue = EventInfo.newBuilder()
                .setId(fifty)
                .setName("event_" + fifty)
                .setEventTime(fifty)
                .build();

        assertThatThrownBy(() -> q.getRecord(null, lookupKey))
                .isExactlyInstanceOf(IllegalArgumentException.class);

        assertThat(q.getRecord(tableName, lookupKey).getPayload()).isEqualTo(expectedValue);

        // Get by secondary index.
        final long fiftyLong = 50L;
        Collection<Message> secondaryIndex = q.getByIndex(tableName, "event_time", fiftyLong).getResult()
                .stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        assertThat(secondaryIndex)
                .hasSize(1)
                .containsExactly(expectedValue);

        // Execute Query. (Scan and filter)
        final int sixty = 60;

        QueryResult<CorfuStoreEntry<Uuid, EventInfo, ManagedResources>> queryResult =
                q.executeQuery(tableName, record -> ((EventInfo) record.getPayload()).getEventTime() >= sixty);
        assertThat(queryResult.getResult().size())
                .isEqualTo(count - sixty);

        assertThat(q.count(tableName, timestamp)).isEqualTo(1);
        assertThat(q.count(tableName)).isEqualTo(count + 1);

        assertThat(q.keySet(tableName, null)).hasSize(count + 1);

        assertThat(corfuStore.listTables(nsxManager))
                .containsExactly(CorfuStoreMetadata.TableName.newBuilder()
                        .setNamespace(nsxManager)
                        .setTableName(tableName)
                        .build());

    }

    /**
     * CorfuStore stores 3 pieces of information - key, value and metadata
     * This test demonstrates how metadata field options esp "version" can be used and verified.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedResources user_1 = ManagedResources.newBuilder().setCreateUser("user_1").build();
        ManagedResources user_2 = ManagedResources.newBuilder().setCreateUser("user_2").build();
        long expectedVersion = 0L;

        corfuStore.tx(nsxManager)
                .create(tableName,
                        key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        user_1)
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_1")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion++).build());

        // Set the version field to the correct value 1 and expect that no exception is thrown
        corfuStore.tx(nsxManager)
                .update(tableName,
                        key1,
                        EventInfo.newBuilder().setName("bcd").build(),
                        ManagedResources.newBuilder().setCreateUser("user_2").setVersion(0L).build())
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion++).build());

        // Now do an updated without setting the version field, and it should not get validated!
        corfuStore.tx(nsxManager)
                .update(tableName,
                        key1,
                        EventInfo.newBuilder().setName("cde").build(),
                        user_2)
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        corfuStore.tx(nsxManager).delete(tableName, key1).commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1)).isNull();
        expectedVersion = 0L;

        corfuStore.tx(nsxManager)
                .update(tableName,
                        key1,
                        EventInfo.newBuilder().setName("def").build(),
                        user_2)
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(ManagedResources.newBuilder(user_2)
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Validate that if Metadata schema is specified, a null metadata is not acceptable
        assertThatThrownBy(() ->
        table.update(key1, EventInfo.getDefaultInstance(), null))
                .isExactlyInstanceOf(RuntimeException.class);

        // Validate that we throw a StaleObject exception if there is an explicit version mismatch
        ManagedResources wrongRevisionMetadata = ManagedResources.newBuilder(user_2)
                .setVersion(2).build();

        assertThatThrownBy(() ->
                table.update(key1, EventInfo.getDefaultInstance(), wrongRevisionMetadata))
                .isExactlyInstanceOf(RuntimeException.class);

        // Verify the table is readable using entryStream()
        final int batchSize = 50;
        Stream<CorfuStoreEntry<Uuid, EventInfo, ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<Uuid, EventInfo, ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<Uuid, EventInfo, ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<Uuid, EventInfo, ManagedResources> entry : partition) {
                assertThat(entry.getKey()).isExactlyInstanceOf(Uuid.class);
                assertThat(entry.getPayload()).isExactlyInstanceOf(EventInfo.class);
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
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        final String nsxManager = "nsx-manager"; // namespace for the table
        final String tableName = "EventInfo"; // table name
        final int numThreads = 10;
        scheduleConcurrently(numThreads, t -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                // Create & Register the table.
                // This is required to initialize the table for the current corfu client.
                corfuStore.openTable(nsxManager, tableName, Uuid.class, EventInfo.class, null,
                        TableOptions.builder().build());
            }

        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);
    }

    /**
     * Demonstrates the checks that the metadata passed into CorfuStore is validated against.
     *
     * @throws Exception
     */
    @Test
    public void checkNullMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
        corfuStore.tx(nsxManager)
                .create(tableName,
                        key1,
                        EventInfo.newBuilder().setName("abc").build(),
                        null)
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();

        corfuStore.tx(nsxManager)
                .update(tableName,
                        key1,
                        EventInfo.newBuilder().setName("bcd").build(),
                        ManagedResources.newBuilder().setCreateUser("testUser").setVersion(1L).build())
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();

        corfuStore.tx(nsxManager)
                .update(tableName,
                        key1,
                        EventInfo.newBuilder().setName("cde").build(),
                        null)
                .commit();
        assertThat(corfuStore.openTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataMergesOldFieldsTest() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, EventInfo, ManagedResources> table = corfuStore.openTable(
                nsxManager,
                tableName,
                Uuid.class,
                EventInfo.class,
                ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        EventInfo value = EventInfo.newBuilder().setName("simpleValue").build();

        long timestamp = System.currentTimeMillis();
        corfuStore.tx(nsxManager)
                .create(tableName, key, value,
                        ManagedResources.newBuilder()
                                .setCreateTimestamp(timestamp).build())
                .commit();

        corfuStore.tx(nsxManager)
                .update(tableName, key, value,
                        ManagedResources.newBuilder().setCreateUser("CreateUser").build())
                .commit();

        CorfuRecord<EventInfo, ManagedResources> record1 = table.get(key);
        assertThat(record1.getMetadata().getCreateTimestamp()).isEqualTo(timestamp);
        assertThat(record1.getMetadata().getCreateUser()).isEqualTo("CreateUser");
    }

    /**
     * This is a research work done to demonstrate how Google DynamicMessage can be used to print/dump
     * the contents of the protobuf store which was written by a fully qualified type.
     *
     * @throws Exception
     */
    @Test
    public void DynamicMessageProtobufTest() throws Exception {
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
        Message m = any.unpack(FirewallRule.class);
        printMessage(data, fileDescriptorProtoMap);
    }

    void printMessage(byte[] data, Map<String, FileDescriptorProto> map) throws Exception {

        FileDescriptor firewallDescriptor = getDescriptors("sample_schema.proto", map);
        DynamicMessage msg = DynamicMessage.parseFrom(firewallDescriptor.findMessageTypeByName("FirewallRule"), data);
        log.info(msg.toString());
    }

    FileDescriptor getDescriptors(String name, Map<String, FileDescriptorProto> map) throws DescriptorValidationException {

        List<FileDescriptor> list = new ArrayList<>();
        for (String s : map.get(name).getDependencyList()) {
            FileDescriptor descriptors = getDescriptors(s, map);
            list.add(descriptors);
        }
        FileDescriptor[] fileDescriptors = list.toArray(new FileDescriptor[list.size()]);
        return FileDescriptor.buildFrom(map.get(name), fileDescriptors);
    }
}
