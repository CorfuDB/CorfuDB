package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        txn.commit();
        long tail1 = shimStore.getHighestSequence(someNamespace, tableName);

        // Take a snapshot to test snapshot isolation transaction
        final CorfuStoreMetadata.Timestamp timestamp = shimStore.getTimestamp();
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
    }

    /**
     * Simple example to see how secondary indexes work. Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testSecondaryIndexes() throws Exception {

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
        Table<UuidMsg, ExampleValue, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleValue.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        UuidMsg key1 = UuidMsg.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        ManagedMetadata user_1 = ManagedMetadata.newBuilder().setCreateUser("user_1").build();

        final long eventTime = 123L;
        UUID randomUUID = UUID.randomUUID();
        ExampleSchemas.Uuid uuidSecondaryKey = ExampleSchemas.Uuid.newBuilder()
                .setMsb(randomUUID.getMostSignificantBits()).setLsb(randomUUID.getLeastSignificantBits())
                .build();

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ExampleValue.newBuilder()
                            .setPayload("abc")
                            .setAnotherKey(eventTime)
                            .setUuid(uuidSecondaryKey)
                            .build(),
                    user_1);
            txn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "anotherKey", eventTime);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getPayload()).isEqualTo("abc");
            readWriteTxn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "uuid", uuidSecondaryKey);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getPayload()).isEqualTo("abc");
            readWriteTxn.commit();
        }
    }

    /**
     * Simple example to see how nested secondary indexes work. Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexes() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        Table<UuidMsg, ExampleValue, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleValue.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        // Create 100 records
        final int totalRecords = 100;
        final long even = 0L;
        final long odd = 1L;
        List<Long> evenRecordIndexes = new ArrayList<>();
        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for(int i=0; i<totalRecords; i++) {
            if(i % 2 == 0) {
                evenRecordIndexes.add(Long.valueOf(i));
            }

            UUID uuid = UUID.randomUUID();
            UuidMsg key = UuidMsg.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, key,
                        ExampleValue.newBuilder()
                                .setPayload("payload_" + i)
                                .setAnotherKey(System.currentTimeMillis())
                                .setEntryIndex(i)
                                .setNonPrimitiveFieldLevel0(ExampleSchemas.NonPrimitiveValue.newBuilder()
                                .setKey1Level1(i % 2 == 0 ? even : odd)
                                .setKey2Level1(ExampleSchemas.NonPrimitiveNestedValue.newBuilder()
                                        .setKey1Level2(i < (totalRecords/2) ? "lower half" : "upper half")
                                        .setLevelNumber(2)
                                        .build()))
                                .build(),
                        user);
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all even entries
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "non_primitive_field_level_0.key_1_level_1", even);
            assertThat(entries.size()).isEqualTo(totalRecords/2);
            Iterator<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> it = entries.iterator();
            while(it.hasNext()) {
                CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata> entry = it.next();
                assertThat(evenRecordIndexes).contains(entry.getPayload().getEntryIndex());
                evenRecordIndexes.remove(entry.getPayload().getEntryIndex());
            }

            assertThat(evenRecordIndexes).isEmpty();
            readWriteTxn.commit();
        }

        // Get by secondary index from second level (nested), retrieve from database 'upper half'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "non_primitive_field_level_0.key_2_level_1.key_1_level_2", "upper half");
            assertThat(entries.size()).isEqualTo(totalRecords/2);
            long sum = 0;
            Iterator<CorfuStoreEntry<UuidMsg, ExampleValue, ManagedMetadata>> it = entries.iterator();
            while(it.hasNext()) {
                sum = sum + it.next().getPayload().getEntryIndex();
            }

            // Assert sum of consecutive numbers of "upper half" match the expected value
            assertThat(sum).isEqualTo(((totalRecords/2) / 2)*((totalRecords/2) + (totalRecords-1)));
            readWriteTxn.commit();
        }
    }

    /**
     * Simple example to see how nested secondary indexes work on repeated fields. Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesRepeatedField() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.ClassRoom, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.ClassRoom.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        // Create records for 40 classRooms
        final int totalClassRooms = 40;
        final long youngStudent = 15L;
        final long olderStudent = 25L;
        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < totalClassRooms; i++) {
            UUID uuid = UUID.randomUUID();
            UuidMsg key = UuidMsg.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, key,
                        ExampleSchemas.ClassRoom.newBuilder()
                                // Student 1 per ClassRoom
                                .addStudents(ExampleSchemas.Student.newBuilder()
                                .setName("MaleStudent_" + i)
                                .setAge(i % 2 == 0 ? youngStudent : olderStudent).build())
                                // Student 2 pero ClassRoom
                                .addStudents(ExampleSchemas.Student.newBuilder()
                                        .setName("FemaleStudent_" + i)
                                        .setAge(i % 2 == 0 ? youngStudent : olderStudent).build())
                                .build(),
                        user);
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all classRooms that have young students
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ClassRoom, ManagedMetadata>> classRooms = readWriteTxn
                    .getByIndex(table, "students.age", youngStudent);
            // Since only even indexed classRooms have youngStudents, we expect half of them to appear
            assertThat(classRooms.size()).isEqualTo(totalClassRooms/2);
            readWriteTxn.commit();
        }
    }

    /**
     * Test the case of a nested secondary index on repeated fields followed by a non-primitive indexed value
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesRepeatedFieldNonPrimitiveIndexed() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "California";
        // Define table name.
        final String tableName = "CA-Networks";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.Network, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Network.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        final int totalNetworks = 10;

        // Even indexed networks will have Router_A
        ExampleSchemas.Router routerA = ExampleSchemas.Router.newBuilder()
                .setBrand("Brand_A")
                .addInterfaces("eth0")
                .build();

        // Odd indexed networks will have Router_B
        ExampleSchemas.Router routerB = ExampleSchemas.Router.newBuilder()
                .setBrand("Brand_B")
                .addInterfaces("wlan0")
                .build();

        // All networks will have a Router_C
        ExampleSchemas.Router routerC = ExampleSchemas.Router.newBuilder()
                .setBrand("Brand_C")
                .addInterfaces("eth1")
                .build();

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < totalNetworks; i++) {
            UUID id = UUID.randomUUID();
            UuidMsg networkId = UuidMsg.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, networkId,
                        ExampleSchemas.Network.newBuilder()
                                // Device 1
                                .addDevices(ExampleSchemas.Device.newBuilder()
                                        .setRouter(i % 2 == 0 ? routerA : routerB)
                                        .build())
                                // Device 2
                                .addDevices(ExampleSchemas.Device.newBuilder()
                                        .setRouter(routerC)
                                        .build())
                                .build(),
                        user);
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all networks which have RouterA
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Network, ManagedMetadata>> networks = readWriteTxn
                    .getByIndex(table, "devices.router", routerA);
            assertThat(networks.size()).isEqualTo(totalNetworks/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all networks which have RouterC
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Network, ManagedMetadata>> networks = readWriteTxn
                    .getByIndex(table, "devices.router", routerC);
            assertThat(networks.size()).isEqualTo(totalNetworks);
            readWriteTxn.commit();
        }
    }

    /**
     * Test the case of a nested secondary index on REPEATED fields followed by a REPEATED non-primitive field which
     * is directly the indexed value
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesWhenIndexedIsNonPrimitiveAndRepeated() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "Companies-Namespace";
        // Define table name.
        final String tableName = "Company";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.Company, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Company.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        final int totalCompanies = 100;

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

        // Even indexed companies will have Office_A and Office_C
        ExampleSchemas.Office office_A = ExampleSchemas.Office.newBuilder()
                .addDepartments(dpt_1)
                .addDepartments(dpt_4)
                .build();

        // Odd indexed companies will have Office_B
        ExampleSchemas.Office office_B = ExampleSchemas.Office.newBuilder()
                .addDepartments(dpt_2)
                .addDepartments(dpt_3)
                .addDepartments(dpt_4)
                .build();

        ExampleSchemas.Office office_C = ExampleSchemas.Office.newBuilder()
                .addDepartments(dpt_1)
                .addDepartments(dpt_4)
                .build();

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < totalCompanies; i++) {
            UUID id = UUID.randomUUID();
            UuidMsg networkId = UuidMsg.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                if (i % 2 == 0) {
                    txn.putRecord(tableName, networkId,
                            ExampleSchemas.Company.newBuilder()
                                    .addOffice(office_A)
                                    .addOffice(office_C)
                                    .build(),
                            user);
                } else {
                    txn.putRecord(tableName, networkId,
                            ExampleSchemas.Company.newBuilder()
                                    .addOffice(office_B)
                                    .build(),
                            user);
                }
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all Companies that have Department of type 1
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType1 = readWriteTxn
                    .getByIndex(table, "office.departments", dpt_1);
            assertThat(companiesDepartmentType1.size()).isEqualTo(totalCompanies/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all Companies that have Department of Type 4 (all)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType4 = readWriteTxn
                    .getByIndex(table, "office.departments", dpt_4);
            assertThat(companiesDepartmentType4.size()).isEqualTo(totalCompanies);
            readWriteTxn.commit();
        }
    }

    /**
     * Simple example to see how nested secondary indexes work on repeated fields, when the repeated field is
     * not the root level but a nested level. Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesNestedRepeatedField() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "Company-Personal";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.Person, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Person.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        // Create 10 records
        final int people = 10;
        final String mobileForEvens = "650-123-4567";
        final String mobileForOdds = "408-987-6543";
        final String mobileCommonBoth = "491-999-1111";

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < people; i++) {
            UUID uuid = UUID.randomUUID();
            UuidMsg key = UuidMsg.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, key,
                        ExampleSchemas.Person.newBuilder()
                                .setName("Name_" + i)
                                .setAge(i)
                                .setPhoneNumber(ExampleSchemas.PhoneNumber.newBuilder()
                                        .setHome(UUID.randomUUID().toString())
                                        .addMobile(i % 2 == 0 ? mobileForEvens : mobileForOdds)
                                        .addMobile(mobileCommonBoth)
                                        .build())
                                .build(),
                        user);
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all even entries
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Person, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "phoneNumber.mobile", mobileForEvens);
            assertThat(entries.size()).isEqualTo(people/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all entries with common mobile number
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Person, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "phoneNumber.mobile", mobileCommonBoth);
            assertThat(entries.size()).isEqualTo(people);
            readWriteTxn.commit();
        }
    }

    /**
     * Example to see how nested secondary indexes work on recursive 'repeated' fields. Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesRecursiveRepeatedFields() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "Offices";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.Office, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Office.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        // Create 10 records
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
            UuidMsg officeId = UuidMsg.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, officeId,
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
                        user);
                txn.commit();
            }
        }

        // Get by secondary index, retrieve from database all offices which have an evenPhoneNumber
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Office, ManagedMetadata>> offices = readWriteTxn
                    .getByIndex(table, "departments.members.phoneNumbers", evenPhoneNumber);
            assertThat(offices.size()).isEqualTo(numOffices/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all entries with common mobile number
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Office, ManagedMetadata>> offices = readWriteTxn
                    .getByIndex(table, "departments.members.phoneNumbers", commonPhoneNumber);
            assertThat(offices.size()).isEqualTo(numOffices);
            readWriteTxn.commit();
        }
    }

    /**
     * Example to see how nested secondary indexes work on repeated fields, when followed by a non-primitive type and ending
     * with a primitive index.
     * Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesRepeatedFieldFollowedByNonPrimitive() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "School";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.School, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.School.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        final int numSchools = 10;
        final long evenNumDesks = 40L;
        final long oddNumDesks = 35L;
        final long noDesks = 0L;
        final long maxNumDesks = 50L;
        String[] others = {"windows", "a/c", "stand up desks", "computers", "tv"};

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < numSchools; i++) {
            UUID id = UUID.randomUUID();
            UuidMsg schoolId = UuidMsg.newBuilder()
                    .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                txn.putRecord(tableName, schoolId,
                        ExampleSchemas.School.newBuilder()
                                // ClassRoom 1
                                .addClassRooms(ExampleSchemas.ClassRoom.newBuilder()
                                        .setClassInfra(ExampleSchemas.Infrastructure.newBuilder()
                                                .setNumberDesks(i % 2 == 0 ? evenNumDesks : oddNumDesks)
                                                .addOthers(others[i % others.length])
                                                .addOthers("Others_" + i)
                                                .build())
                                        .build())
                                // ClassRoom 2
                                .addClassRooms(ExampleSchemas.ClassRoom.newBuilder()
                                        .setClassInfra(ExampleSchemas.Infrastructure.newBuilder()
                                                .setNumberDesks(maxNumDesks)
                                                .build())
                                        .build())
                                .build(),
                        user);
                txn.commit();
            }
        }

        // Add one additional school with 1 classRoom and no Desks
        UUID id = UUID.randomUUID();
        UuidMsg schoolId = UuidMsg.newBuilder()
                .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                .build();

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, schoolId,
                    ExampleSchemas.School.newBuilder()
                            // ClassRoom 1
                            .addClassRooms(ExampleSchemas.ClassRoom.newBuilder()
                                    .setClassInfra(ExampleSchemas.Infrastructure.newBuilder()
                                            .setNumberDesks(noDesks)
                                            .build())
                                    .build())
                            .build(),
                    user);
            txn.commit();
        }

        // Get by secondary index, retrieve number of schools that have classrooms with odd number of desks
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.numberDesks", oddNumDesks);
            assertThat(schools.size()).isEqualTo(numSchools/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of schools that have classrooms with no desks
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.numberDesks", noDesks);
            assertThat(schools.size()).isEqualTo(1);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of schools that have classrooms with computers (repeated primitive field)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.others", "computers");
            assertThat(schools.size()).isEqualTo(numSchools/others.length);
            readWriteTxn.commit();
        }
    }

    /**
     * Example to see how nested secondary indexes work when a secondary index is set on a 'oneOf' field.
     * This test combines all possibilities of secondary indexes on 'oneOf' properties:
     * (1) Index is set directly on a 'oneOf' attribute that is not nested (first level of proto definition)
     * (2) Index is set directly on a 'oneOf' nested attribute (not the first level of proto definition)
     * (3) Index is set on a 'oneOf' field contained within a repeated field, covering all these scenarios:
     *      (a) Index is on a primitive type (within the oneOf)
     *      (b) Index non-Primitive type (within the oneOf)
     *      (c) Index is on a repeated field of the 'oneOf' attribute (e.g., in example_schemas.proto consider
     *         the case of contacts.number.mobile)
     *
     * Please see example_schemas.proto: ContactBook
     *
     * In this test, we create 3 contact books: work, friends & family
     * A 'contact book' is composed of a list of 'contact', each contact has a 'name' and its 'contactInformation'
     * is either an 'address' (which has a field city) or a 'phone number' (contactInformation is 'oneOf')
     * A 'common' phone number, is a number shared across different contacts
     *
     * * ContactBookWork (4 contacts):
     *   - 1 with San Francisco Address
     *   - 3 with Phone Number: 1 contact with common phone number (home and mobile)
     *                          1 work contact with unique phone number
     *                          1 contact with a common mobile number (its own home number)
     * * ContactBookFriends (3 contacts):
     *   - 1 with San Francisco Address
     *   - 2 with Phone Number:
     *                          1 friend contact with unique phone number
     *                          1 contact with a common mobile number (its own home number)
     *
     * * ContactBookFamily (4 contacts):
     *   - 1 with Los Angeles Address
     *   - 3 with Phone Number: 1 contact with common phone number (home and mobile)
     *                          1 work contact with unique phone number
     *                          1 contact with a common mobile number (its own home number)
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexesOneOfFields() throws Exception {

        CorfuRuntime corfuRuntime = getTestRuntime();
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        final String someNamespace = "IT-namespace";
        final String tableName = "MyContactBooks";

        // Create & Register the table, that contains all 'ContactBook'
        Table<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.ContactBook.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        // Common mobile number (nested repeated field in a 'oneOf' field) which will be present across all contactBooks
        final String commonMobile = "408-2219873";

        // Create contacts with address as contactInformation
        List<ExampleSchemas.Contact> contactsWithAddress = getContactsWithAddress();

        // Create contacts with phoneNumber as contactInformation
        List<ExampleSchemas.Contact> contactsWithPhone = getContactsWithPhone(commonMobile);

        // (1) Contact Book for Work Colleagues: contacts in SFO, with PhoneNumber1, PhoneNumber2
        int index = 1;
        UuidMsg contactBookWorkId = getUuid();
        final String contactBookWorkName = "Work Contacts";
        ExampleSchemas.ContactBook contactBookWork = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setName(contactBookWorkName).build())
                .setBrand("Bloom Daily Planners Contacts")
                .addContacts(contactsWithAddress.get(index - 1)) // San Francisco Address
                .addContacts(contactsWithPhone.get(0)) // Common phone number (work and family)
                .addContacts(contactsWithPhone.get(index)) // Unique work number with common mobile
                .build();

        // (2) Contact Book for Friends
        index++;
        UuidMsg contactBookFriendsId = getUuid();
        ExampleSchemas.ContactBook contactBookFriends = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setUuid(contactBookFriendsId.toString()).build())
                .setApplication("iPhone")
                .addContacts(contactsWithAddress.get(index - 1)) // San Francisco Address
                .addContacts(contactsWithPhone.get(index)) // Unique friends number with common mobile
                .build();

        // (3) Contact Book for Family
        index++;
        UuidMsg contactBookFamilyId = getUuid();
        ExampleSchemas.ContactBook contactBookFamily = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setUuid(contactBookFamilyId.toString()).build())
                .setApplication("iPhone")
                .addContacts(contactsWithAddress.get(index - 1)) // Los Angeles Address
                .addContacts(contactsWithPhone.get(index)) // Unique family number with common mobile
                .addContacts(contactsWithPhone.get(++index)) // Unique family with unique phone number
                .addContacts(contactsWithPhone.get(++index)) // Common phone number (work and family)
                .build();

        // Add Contact Books (work, friends, family)
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, contactBookWorkId, contactBookWork, user);
            txn.putRecord(tableName, contactBookFriendsId, contactBookFriends, user);
            txn.putRecord(tableName, contactBookFamilyId, contactBookFamily, user);
            txn.commit();
        }

        List<UuidMsg> contactBooksWithAddressSFO = Arrays.asList(contactBookWorkId, contactBookFriendsId);
        List<UuidMsg> contactBooksWithCommonNumber = Arrays.asList(contactBookWorkId, contactBookFamilyId);
        List<UuidMsg> contactBooksWithCommonMobile = Arrays.asList(contactBookWorkId, contactBookFriendsId, contactBookFamilyId);
        List<UuidMsg> contactBooksIPhone = Arrays.asList(contactBookFriendsId, contactBookFamilyId);


        // Get by secondary index, retrieve number of contactBooks that have contacts with address in San Francisco
        // only contactBookWork and contactBookFriends have contacts with address in SFO.
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "city", "San Francisco");
            assertThat(contactBooks.size()).isEqualTo(contactBooksWithAddressSFO.size());
            contactBooks.forEach(cb -> assertThat(contactBooksWithAddressSFO).contains(cb.getKey()));
            readWriteTxn.commit();
        }

        ExampleSchemas.PhoneNumber phoneNumberCommon = ExampleSchemas.PhoneNumber.newBuilder()
                .setHome("352-546-890")
                .addMobile("611-9458741")
                .build();

        // Get by secondary index, retrieve number of contactBooks that have contacts with 'phoneNumberAllBooks'
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "number", phoneNumberCommon);
            assertThat(contactBooks.size()).isEqualTo(contactBooksWithCommonNumber.size());
            contactBooks.forEach(cb -> assertThat(contactBooksWithCommonNumber).contains(cb.getKey()));
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of contactBooks that have contacts with 'commonMobile'
        // (a contained repeated field of a 'oneOf' property
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "mobile", commonMobile);
            assertThat(contactBooks.size()).isEqualTo(contactBooksWithCommonMobile.size());
            contactBooks.forEach(cb -> assertThat(contactBooksWithCommonMobile).contains(cb.getKey()));
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of contactBooks that are named contactBookWorkName
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "name", contactBookWorkName);
            assertThat(contactBooks.size()).isEqualTo(1);
            contactBooks.forEach(cb -> cb.getPayload().getId().getName().equals(contactBookWorkName));
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of contactBooks that are "iPhone" contact books
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "application", "iPhone");
            assertThat(contactBooks.size()).isEqualTo(contactBooksIPhone.size());
            contactBooks.forEach(cb -> assertThat(contactBooksIPhone).contains(cb.getKey()));
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of contactBooks that have an empty photo (only John in work book)
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> contactBooks = readWriteTxn
                    .getByIndex(table, "file", "/images/empty.png");
            assertThat(contactBooks.size()).isEqualTo(1);
            contactBooks.forEach(cb -> assertThat(cb.getKey()).isEqualTo(contactBookWorkId));
            readWriteTxn.commit();
        }
    }

    /**
     * Simple example to prove an invalid nested secondary index definition will
     * throw an error on openTable (the name is invalid from the root of the secondary index).
     * Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testInvalidNestedSecondaryIndexRoot() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        assertThrows(IllegalArgumentException.class, () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.InvalidExampleValue.class,
                ManagedMetadata.class,
                TableOptions.builder().build()));
    }

    /**
     * Simple example to prove an invalid nested secondary index definition will
     * throw an error on openTable (the name is invalid from lower in the chain of the secondary index definition).
     * Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testInvalidNestedSecondaryIndexLowerLevel() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        assertThrows(IllegalArgumentException.class, () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.InvalidNestedSecondaryIndex.class,
                ManagedMetadata.class,
                TableOptions.builder().build()));
    }

    /**
     * Simple example to prove an invalid nested secondary index definition will
     * throw an error on openTable (the full path is invalid as it is indexed beyond a primitive type).
     * Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testInvalidNestedSecondaryIndexFullPath() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        assertThrows(UnsupportedOperationException.class, () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.InvalidFullNestedSecondaryIndex.class,
                ManagedMetadata.class,
                TableOptions.builder().build()));
    }

    /**
     * Simple example to prove we can access a secondary index based on a custom alias or the default alias.
     *
     * @throws Exception exception
     */
    @Test
    public void testSecondaryIndexAlias() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "alias-namespace";
        // Define table name.
        final String tableName = "Adults";
        final int adultCount = 50;
        final long adultBaseAge = 30L;
        final long kidsBaseAge = 4L;

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.Adult, ManagedMetadata> adultsTable = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Adult.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < adultCount; i++) {
            UUID adultId = UUID.randomUUID();
            UuidMsg adultKey = UuidMsg.newBuilder()
                    .setMsb(adultId.getMostSignificantBits()).setLsb(adultId.getLeastSignificantBits())
                    .build();

            try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
                long adultAge = i % 2 == 0 ? adultBaseAge : adultBaseAge*2;
                long kidsAge = i % 2 == 0 ? kidsBaseAge : kidsBaseAge*2;
                txn.putRecord(tableName, adultKey,
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
                        user);
                txn.commit();
            }
        }


        // Get by secondary index (default alias), retrieve from database all adults with adultsBaseAge
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "age", adultBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (using fully qualified name), retrieve from database all adults with adultsBaseAge
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "person.age", adultBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age 'kidsBaseAge'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "kidsAge", kidsBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (fully qualified name), retrieve from database all adults with kids on age 'kidsBaseAge'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "person.children.child.age", kidsBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age '2' (non existent)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "kidsAge", 2);
            assertThat(entries.size()).isZero();
            readWriteTxn.commit();
        }
    }

    /**
     * Simple example to prove we can't register a table with non unique (default) alias
     *
     * @throws Exception
     */
    @Test
    public void testInvalidAlias() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "InvalidAdultTable";

        // Create & Register the table.
        assertThrows(IllegalArgumentException.class, () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.InvalidAdultDefaultIndexName.class,
                ManagedMetadata.class,
                TableOptions.builder().build()));
    }

    /**
     * Simple example to prove we can't register a table with non-unique alias (user-defined)
     *
     * @throws Exception
     */
    @Test
    public void testInvalidAliasUserDefined() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "InvalidAdultTable";

        // Create & Register the table.
        assertThrows(IllegalArgumentException.class, () -> shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.InvalidAdultCustomIndexName.class,
                ManagedMetadata.class,
                TableOptions.builder().build()));
    }

    /**
     * Simple example to prove that a nested secondary index definition with no inner object definition, defaults to
     * the behavior of the secondary_key annotation (true/false).
     * Please see example_schemas.proto.
     *
     * @throws Exception exception
     */
    @Test
    public void testNestedSecondaryIndexNotNested() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        Table<UuidMsg, ExampleSchemas.NotNestedSecondaryIndex, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.NotNestedSecondaryIndex.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        UUID id = UUID.randomUUID();
        UuidMsg key1 = UuidMsg.newBuilder()
                .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                .build();
        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        try (ManagedTxnContext txn = shimStore.txn(someNamespace)) {
            txn.putRecord(tableName, key1,
                    ExampleSchemas.NotNestedSecondaryIndex.newBuilder()
                            .setField1("record_1")
                            .setField2(ExampleSchemas.NonPrimitiveValue.newBuilder()
                                    .setKey1Level1(0L)
                                    .setKey2Level1(ExampleSchemas.NonPrimitiveNestedValue.newBuilder()
                                            .setKey1Level2("random")
                                            .setLevelNumber(2)
                                            .build()))
                            .setField3(0L)
                            .build(),
                    user);
            txn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.NotNestedSecondaryIndex, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "field3", 0L);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getField1()).isEqualTo("record_1");
            readWriteTxn.commit();
        }
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

        UuidMsg key = UuidMsg.newBuilder().setLsb(0L).setMsb(0L).build();
        ExampleSchemas.ManagedMetadata value = ExampleSchemas.ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

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
                    final long streamOffset = txn.commit();
                    validator.add(i);
                    log.debug("ENQ: {} => {} at {}", i, queueData, streamOffset);
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
    }

    // ****** Utility Functions *******

    private UuidMsg getUuid() {
        UUID id = UUID.randomUUID();
        return UuidMsg.newBuilder()
                .setMsb(id.getMostSignificantBits())
                .setLsb(id.getLeastSignificantBits())
                .build();
    }

    private List<ExampleSchemas.Contact> getContactsWithPhone(String commonMobile) {
        // Phone Number common across all three ContactBooks
        final ExampleSchemas.PhoneNumber phoneNumberCommon = ExampleSchemas.PhoneNumber.newBuilder()
                .setHome("352-546-890")
                .addMobile("611-9458741")
                .build();

        final ExampleSchemas.PhoneNumber phoneNumberWorkBook = ExampleSchemas.PhoneNumber.newBuilder()
                .addMobile(commonMobile)
                .addMobile("708-7841235")
                .setHome("921-3456709")
                .build();

        final ExampleSchemas.PhoneNumber phoneNumberFriendsBook = ExampleSchemas.PhoneNumber.newBuilder()
                .setHome("987-7846985")
                .addMobile(commonMobile)
                .build();

        final ExampleSchemas.PhoneNumber phoneNumberFamilyBook = ExampleSchemas.PhoneNumber.newBuilder()
                .addMobile(commonMobile)
                .build();

        final ExampleSchemas.PhoneNumber phoneNumberUnique = ExampleSchemas.PhoneNumber.newBuilder()
                .setHome("111-11-1111")
                .build();

        List<String> namesForPhones = Arrays.asList("Mark & Lisa", "Mary (work)", "Jane (friend)", "Steven (cousin)", "Matt (uncle)", "Lisa (aunt)");
        List<ExampleSchemas.PhoneNumber> phoneNumbers = Arrays.asList(phoneNumberCommon, phoneNumberWorkBook,
                phoneNumberFriendsBook, phoneNumberFamilyBook, phoneNumberUnique, phoneNumberCommon);
        List<ExampleSchemas.Contact> contactsWithPhone = new ArrayList<>();

        for (int i = 0; i < namesForPhones.size(); i++) {
            contactsWithPhone.add(ExampleSchemas.Contact.newBuilder()
                    .setPhoto(ExampleSchemas.Photo.newBuilder().setDescription("No Description Available").build())
                    .setName(namesForPhones.get(i))
                    .setNumber(phoneNumbers.get(i))
                    .build());
        }

        return contactsWithPhone;
    }

    /**
     * Return a list of 'Contacts' with address as 'contactInformation'
     */
    private List<ExampleSchemas.Contact> getContactsWithAddress() {
        final ExampleSchemas.Contact contactSFOWork = ExampleSchemas.Contact.newBuilder()
                .setName("Jhon")
                .setPhoto(ExampleSchemas.Photo.newBuilder().setFile("/images/empty.png").build())
                .setAddress(ExampleSchemas.Address.newBuilder()
                        .setNumber(1)
                        .setStreet("Arches")
                        .setUnit("108")
                        .setCity("San Francisco")
                        .build())
                .build();

        final ExampleSchemas.Contact contactSFOFriends = ExampleSchemas.Contact.newBuilder()
                .setName("Marc")
                .setPhoto(ExampleSchemas.Photo.newBuilder().setFile("/images/marc.png").build())
                .setAddress(ExampleSchemas.Address.newBuilder()
                        .setNumber(1)
                        .setStreet("El Camino Real")
                        .setUnit("4256")
                        .setCity("San Francisco")
                        .build())
                .build();

        final ExampleSchemas.Contact contactLAFamily = ExampleSchemas.Contact.newBuilder()
                .setName("Jane")
                .setPhoto(ExampleSchemas.Photo.newBuilder().setFile("/images/jane.png").build())
                .setAddress(ExampleSchemas.Address.newBuilder()
                        .setNumber(1)
                        .setStreet("Hollywood Blvd.")
                        .setUnit("214")
                        .setCity("Los Angeles")
                        .build())
                .build();

        return Arrays.asList(contactSFOWork, contactSFOFriends, contactLAFamily);
    }
}
