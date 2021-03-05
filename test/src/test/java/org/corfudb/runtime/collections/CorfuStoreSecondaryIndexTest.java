package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.ExampleSchemas.Adult;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This suite of tests simulate the use of secondary indexes through CorfuStore.
 * <p>
 * Created by annym
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class CorfuStoreSecondaryIndexTest extends AbstractViewTest {

    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
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

        List<ExampleSchemas.Department> departments = createApartments();

        createOffices(departments, totalCompanies, shimStore, someNamespace, tableName);

        // Get by secondary index, retrieve from database all Companies that have Department of type 1
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType1 = readWriteTxn
                    .getByIndex(table, "office.departments", departments.get(0));
            assertThat(companiesDepartmentType1.size()).isEqualTo(totalCompanies/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all Companies that have Department of Type 4 (all)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType4 = readWriteTxn
                    .getByIndex(table, "office.departments", departments.get(3));
            assertThat(companiesDepartmentType4.size()).isEqualTo(totalCompanies);
            readWriteTxn.commit();
        }
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

    private void createOffices(List<ExampleSchemas.Department> departments, int totalCompanies, CorfuStoreShim shimStore,
                                   String someNamespace, String tableName) {

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
        final long oddNumDesks = 35L;
        final long noDesks = 1L;
        String[] others = {"windows", "a/c", "stand up desks", "computers", "tv"};

        createSchools(shimStore, someNamespace, tableName, numSchools, oddNumDesks, noDesks, others);

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

    private void createSchools(CorfuStoreShim shimStore, String someNamespace, String tableName,
                               int numSchools, long oddNumDesks, long noDesks, String[] others) {

        final long evenNumDesks = 40L;
        final long maxNumDesks = 50L;

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

        // Common mobile number (nested repeated field in a 'oneOf' field) which will be present across all contactBooks
        final String commonMobile = "408-2219873";

        // Create Contact Books: Work, Friends and Family
        UuidMsg contactBookWorkId = getUuid();
        UuidMsg contactBookFriendsId = getUuid();
        UuidMsg contactBookFamilyId = getUuid();
        final String contactBookWorkName = "Work Contacts";

        createContactBooks(shimStore, someNamespace, tableName, commonMobile,
                Arrays.asList(contactBookWorkId, contactBookFriendsId, contactBookFamilyId), contactBookWorkName);

        List<UuidMsg> contactBooksWithAddressSFO = Arrays.asList(contactBookWorkId, contactBookFriendsId);
        List<UuidMsg> contactBooksWithCommonNumber = Arrays.asList(contactBookWorkId, contactBookFamilyId);
        List<UuidMsg> contactBooksWithCommonMobile = Arrays.asList(contactBookWorkId, contactBookFriendsId, contactBookFamilyId);
        List<UuidMsg> contactBooksIPhone = Arrays.asList(contactBookFriendsId, contactBookFamilyId);

        // Get by secondary index, retrieve number of contactBooks that have contacts with address in San Francisco
        // only contactBookWork and contactBookFriends have contacts with address in SFO.
        getContactsBookBySecondaryIndexString(shimStore, someNamespace, table, "city", "San Francisco", contactBooksWithAddressSFO);

        ExampleSchemas.PhoneNumber phoneNumberCommon = ExampleSchemas.PhoneNumber.newBuilder()
                .setHome("352-546-890")
                .addMobile("611-9458741")
                .build();

        // Get by secondary index, retrieve number of contactBooks that have contacts with 'phoneNumberAllBooks'
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> books = readWriteTxn
                    .getByIndex(table, "number", phoneNumberCommon);
            assertThat(books.size()).isEqualTo(contactBooksWithCommonNumber.size());
            books.forEach(cb -> assertThat(contactBooksWithCommonNumber).contains(cb.getKey()));
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of contactBooks that have contacts with 'commonMobile'
        // (a contained repeated field of a 'oneOf' property
        getContactsBookBySecondaryIndexString(shimStore, someNamespace, table, "mobile", commonMobile,
                contactBooksWithCommonMobile);

        // Get by secondary index, retrieve number of contactBooks that are named contactBookWorkName
        getContactsBookBySecondaryIndexString(shimStore, someNamespace, table, "name", contactBookWorkName,
                Arrays.asList(contactBookWorkId));

        // Get by secondary index, retrieve number of contactBooks that are "iPhone" contact books
        getContactsBookBySecondaryIndexString(shimStore, someNamespace, table, "application",
                "iPhone", contactBooksIPhone);

        // Get by secondary index, retrieve number of contactBooks that have an empty photo (only John in work book)
        getContactsBookBySecondaryIndexString(shimStore, someNamespace, table, "file", "/images/empty.png",
                Arrays.asList(contactBookWorkId));
    }

    private void getContactsBookBySecondaryIndexString(CorfuStoreShim shimStore, String someNamespace,
                                                       Table<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata> table,
                                                       String indexName, String indexKey, List<UuidMsg> expectedContactBooks) {
        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.ContactBook, ManagedMetadata>> books = readWriteTxn
                    .getByIndex(table, indexName, indexKey);
            assertThat(books.size()).isEqualTo(expectedContactBooks.size());
            books.forEach(cb -> assertThat(expectedContactBooks).contains(cb.getKey()));
            readWriteTxn.commit();
        }
    }

    private void createContactBooks(CorfuStoreShim shimStore, String someNamespace, String tableName, String commonMobile,
                                                             List<UuidMsg> contactBooksIds, String contactBookWorkName) {

        // Create contacts with address as contactInformation
        List<ExampleSchemas.Contact> contactsWithAddress = getContactsWithAddress();

        // Create contacts with phoneNumber as contactInformation
        List<ExampleSchemas.Contact> contactsWithPhone = getContactsWithPhone(commonMobile);

        // (1) Contact Book for Work Colleagues: contacts in SFO, with PhoneNumber1, PhoneNumber2
        int index = 1;
        ExampleSchemas.ContactBook contactBookWork = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setName(contactBookWorkName).build())
                .setBrand("Bloom Daily Planners Contacts")
                .addContacts(contactsWithAddress.get(index - 1)) // San Francisco Address
                .addContacts(contactsWithPhone.get(0)) // Common phone number (work and family)
                .addContacts(contactsWithPhone.get(index)) // Unique work number with common mobile
                .build();

        // (2) Contact Book for Friends
        index++;
        ExampleSchemas.ContactBook contactBookFriends = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setUuid(contactBooksIds.get(1).toString()).build())
                .setApplication("iPhone")
                .addContacts(contactsWithAddress.get(index - 1)) // San Francisco Address
                .addContacts(contactsWithPhone.get(index)) // Unique friends number with common mobile
                .build();

        // (3) Contact Book for Family
        index++;
        ExampleSchemas.ContactBook contactBookFamily = ExampleSchemas.ContactBook.newBuilder()
                .setId(ExampleSchemas.ContactBookId.newBuilder().setUuid(contactBooksIds.get(2).toString()).build())
                .setApplication("iPhone")
                .addContacts(contactsWithAddress.get(index - 1)) // Los Angeles Address
                .addContacts(contactsWithPhone.get(index)) // Unique family number with common mobile
                .addContacts(contactsWithPhone.get(++index)) // Unique family with unique phone number
                .addContacts(contactsWithPhone.get(++index)) // Common phone number (work and family)
                .build();

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        // Add Contact Books (work, friends, family)
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, contactBooksIds.get(0), contactBookWork, user);
            txn.putRecord(tableName, contactBooksIds.get(1), contactBookFriends, user);
            txn.putRecord(tableName, contactBooksIds.get(2), contactBookFamily, user);
            txn.commit();
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
                                    .setKey1Level1(1L)
                                    .setKey2Level1(ExampleSchemas.NonPrimitiveNestedValue.newBuilder()
                                            .setKey1Level2("random")
                                            .setLevelNumber(2L)
                                            .build()))
                            .setField3(1L)
                            .build(),
                    user);
            txn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.NotNestedSecondaryIndex, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "field3", 1L);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getField1()).isEqualTo("record_1");
            readWriteTxn.commit();
        }
    }

    /**
     * Test the case where a nested secondary key is not set (as it is an optional field)
     *  Verify the transaction commits despite the absence of the 'optional' secondary key.
     */
    @Test
    public void testUnsetSecondaryKeyField() throws Exception {

        final String someNamespace = "ut-namespace";
        final String tableName = "AdultsTable";

        // Get a Corfu Runtime instance and create Corfu Store
        CorfuRuntime corfuRuntime = getTestRuntime();
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);
        Map<UuidMsg, ExampleSchemas.Adult> adults = new HashMap<>();

        // Create & Register the table.
        shimStore.openTable(
                someNamespace,
                tableName,
                UuidMsg.class,
                ExampleSchemas.Adult.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        // Create 1st person, with 'NO' secondary index field set (no age, children, company id or address id)
        ExampleSchemas.Adult ricky = Adult.newBuilder()
                .setPerson(ExampleSchemas.Person.newBuilder()
                        .setName("Ricky")
                        .build())
                .build();
        UuidMsg idRicky = createAdult(shimStore, tableName, someNamespace, ricky);
        adults.put(idRicky, ricky);

        // Create 2nd person, with one secondary index field set (age)
        // and all others unset (differently from person 1, this person will have all nested fields set, up to the
        // immediate secondary index field, which will be unset), e.g., it has a Company instance but with no
        // company_id field (which is the secondary index).
        final long ageAdult = 45L;
        ExampleSchemas.Adult mary = ExampleSchemas.Adult.newBuilder()
                .setPerson(ExampleSchemas.Person.newBuilder()
                        .setName("Mary Lu")
                        .setAge(ageAdult)
                        .setChildren(ExampleSchemas.Children.newBuilder()
                                .addChild(ExampleSchemas.Child.newBuilder()
                                        .setName("Mark Lu")
                                        // No 'age' (secondary index)
                                        .build())
                                .build())
                        .build())
                .setWork(ExampleSchemas.Company.newBuilder()
                        // No 'company_id' (secondary index)
                        .addOffice(ExampleSchemas.Office.newBuilder()
                                .setOfficeNickname("Mary's Office")
                                .build())
                        .build())
                .addAddresses(ExampleSchemas.Address.newBuilder()
                        .setCity("Houston")
                        // No 'id' (secondary index)
                        .build())
                .build();
        UuidMsg idMary = createAdult(shimStore, tableName, someNamespace, mary);
        adults.put(idMary, mary);

        // Create 3rd person, with all secondary index fields set
        final long ageChild = 14L;
        ExampleSchemas.Adult kate = ExampleSchemas.Adult.newBuilder()
                .setPerson(ExampleSchemas.Person.newBuilder()
                        .setName("Kate Peterson")
                        .setAge(ageAdult)
                        .setChildren(ExampleSchemas.Children.newBuilder()
                                .addChild(ExampleSchemas.Child.newBuilder()
                                        .setName("Kyle Peterson")
                                        .setAge(ageChild)
                                        .build())
                                .addChild(ExampleSchemas.Child.newBuilder()
                                        .setName("Kia Peterson")
                                        .setAge(0L)
                                        .build())
                                .build())
                        .build())
                .setWork(ExampleSchemas.Company.newBuilder()
                        .setCompanyId(ExampleSchemas.Uuid.newBuilder()
                                .setMsb(1L)
                                .setLsb(1L)
                                .build())
                        .addOffice(ExampleSchemas.Office.newBuilder()
                                .setOfficeNickname("Kate's Office")
                                .build())
                        .build())
                .addAddresses(ExampleSchemas.Address.newBuilder()
                        .setUniqueAddressId(ExampleSchemas.Uuid.newBuilder()
                                .setMsb(1L)
                                .setLsb(1L)
                                .build())
                        .build())
                .build();
        UuidMsg idKate = createAdult(shimStore, tableName, someNamespace, kate);
        adults.put(idKate, kate);

        List<String> adults45y = Arrays.asList(mary.getPerson().getName(), kate.getPerson().getName());

        // Verify all entries were added and indexed correctly
        verifySecondaryIndex(shimStore, someNamespace, tableName, adults, adults45y, ageAdult,
                ageChild, Arrays.asList(idRicky, idMary, idKate));
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

    private UuidMsg createAdult(CorfuStoreShim shimStore, String tableName, String namespace, ExampleSchemas.Adult adult) {
        UUID id = UUID.randomUUID();
        UuidMsg key = UuidMsg.newBuilder()
                .setMsb(id.getMostSignificantBits()).setLsb(id.getLeastSignificantBits())
                .build();
        try (ManagedTxnContext txn = shimStore.txn(namespace)) {
            txn.putRecord(tableName, key, adult,
                    ManagedMetadata.newBuilder().setCreateUser("user_UT").build());
            txn.commit();
        }

        return key;
    }

    private void verifySecondaryIndex(CorfuStoreShim shimStore, String someNamespace, String tableName,
                                      Map<UuidMsg, Adult> adults, List<String> adults45y, long ageAdult,
                                      long ageChild, List<UuidMsg> adultsId) {
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {

            // Verify index by age for a known value
            List<CorfuStoreEntry<UuidMsg, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(tableName, "age", ageAdult);
            assertThat(entries.size()).isEqualTo(adults45y.size());
            assertThat(adults45y).contains(entries.get(0).getPayload().getPerson().getName());
            assertThat(adults45y).contains(entries.get(1).getPayload().getPerson().getName());

            // Verify adults with 'NO AGE' (adults with no age are those for which the field age
            // was not set or explicitly set to 0). Consider proto3 sets default values for unset fields.
            entries = readWriteTxn.getByIndex(tableName, "age", 0L);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload()).isEqualTo(adults.get(adultsId.get(0)));

            // Verify adults with no child (though Ricky has no child, note that child is a repeated property
            // under Children message type, so in this case, child does not account as an unset field because
            // its parent field does not even exist)
            entries = readWriteTxn.getByIndex(tableName, "child", null);
            assertThat(entries.size()).isEqualTo(0);

            // Verify adults with children with 'ageChild'
            entries = readWriteTxn.getByIndex(tableName, "kidsAge", ageChild);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload()).isEqualTo(adults.get(adultsId.get(2)));

            // Verify adults with child with no age (unset or explicitly set to the default value 0L)
            // Mary's child age is unset (defaults to 0L), Kate's child age is explicitly set to 0L (newborn)
            entries = readWriteTxn.getByIndex(tableName, "kidsAge", 0L);
            assertThat(entries.size()).isEqualTo(2);
            assertThat(adults45y).contains(entries.get(0).getPayload().getPerson().getName());
            assertThat(adults45y).contains(entries.get(1).getPayload().getPerson().getName());

            // Verify adults with no explicit address Id, are not indexed by proto's default value
            entries = readWriteTxn.getByIndex(tableName, "address", null);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload()).isEqualTo(adults.get(adultsId.get(1)));

            // Verify adults working at company with company_id = (1, 1)
            entries = readWriteTxn.getByIndex(tableName, "company_id",
                    ExampleSchemas.Uuid.newBuilder().setLsb(1L).setMsb(1L).build());
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload()).isEqualTo(adults.get(adultsId.get(2)));

            // Verify adults working at companies with no company_id
            entries = readWriteTxn.getByIndex(tableName, "company_id", null);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload()).isEqualTo(adults.get(adultsId.get(1)));

            readWriteTxn.commit();
        }
    }
}
