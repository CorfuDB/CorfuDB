package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.exceptions.StaleRevisionUpdateException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.Messages.Uuid;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

import java.util.ArrayList;
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
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
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
        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
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
            Uuid key2 = null;
            assertThatThrownBy( () -> readWriteTxn.putRecord(tableName, key2, null, null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }
        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleValue, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ExampleValue.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "anotherKey", eventTime);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getPayload()).isEqualTo("abc");
            readWriteTxn.commit();
        }

        try (ManagedTxnContext readWriteTxn = shimStore.tx(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "uuid", uuidSecondaryKey);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getPayload()).isEqualTo("abc");
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleValue, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid key = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "non_primitive_field_level_0.key_1_level_1", even);
            assertThat(entries.size()).isEqualTo(totalRecords/2);
            Iterator<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> it = entries.iterator();
            while(it.hasNext()) {
                CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata> entry = it.next();
                assertThat(evenRecordIndexes).contains(entry.getPayload().getEntryIndex());
                evenRecordIndexes.remove(entry.getPayload().getEntryIndex());
            }

            assertThat(evenRecordIndexes).isEmpty();
            readWriteTxn.commit();
        }

        // Get by secondary index from second level (nested), retrieve from database 'upper half'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "non_primitive_field_level_0.key_2_level_1.key_1_level_2", "upper half");
            assertThat(entries.size()).isEqualTo(totalRecords/2);
            long sum = 0;
            Iterator<CorfuStoreEntry<Uuid, ExampleValue, ManagedMetadata>> it = entries.iterator();
            while(it.hasNext()) {
                sum = sum + it.next().getPayload().getEntryIndex();
            }

            // Assert sum of consecutive numbers of "upper half" match the expected value
            assertThat(sum).isEqualTo(((totalRecords/2) / 2)*((totalRecords/2) + (totalRecords-1)));
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.ClassRoom, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid key = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.ClassRoom, ManagedMetadata>> classRooms = readWriteTxn
                    .getByIndex(table, "students.age", youngStudent);
            // Since only even indexed classRooms have youngStudents, we expect half of them to appear
            assertThat(classRooms.size()).isEqualTo(totalClassRooms/2);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.Network, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid networkId = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Network, ManagedMetadata>> networks = readWriteTxn
                    .getByIndex(table, "devices.router", routerA);
            assertThat(networks.size()).isEqualTo(totalNetworks/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all networks which have RouterC
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Network, ManagedMetadata>> networks = readWriteTxn
                    .getByIndex(table, "devices.router", routerC);
            assertThat(networks.size()).isEqualTo(totalNetworks);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.Company, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid networkId = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType1 = readWriteTxn
                    .getByIndex(table, "office.departments", dpt_1);
            assertThat(companiesDepartmentType1.size()).isEqualTo(totalCompanies/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all Companies that have Department of Type 4 (all)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Company, ManagedMetadata>> companiesDepartmentType4 = readWriteTxn
                    .getByIndex(table, "office.departments", dpt_4);
            assertThat(companiesDepartmentType4.size()).isEqualTo(totalCompanies);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.Person, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid key = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Person, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "phoneNumber.mobile", mobileForEvens);
            assertThat(entries.size()).isEqualTo(people/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all entries with common mobile number
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Person, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "phoneNumber.mobile", mobileCommonBoth);
            assertThat(entries.size()).isEqualTo(people);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.Office, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid officeId = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Office, ManagedMetadata>> offices = readWriteTxn
                    .getByIndex(table, "departments.members.phoneNumbers", evenPhoneNumber);
            assertThat(offices.size()).isEqualTo(numOffices/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve from database all entries with common mobile number
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Office, ManagedMetadata>> offices = readWriteTxn
                    .getByIndex(table, "departments.members.phoneNumbers", commonPhoneNumber);
            assertThat(offices.size()).isEqualTo(numOffices);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
        Table<Uuid, ExampleSchemas.School, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
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
            Uuid schoolId = Uuid.newBuilder()
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
        Uuid schoolId = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.numberDesks", oddNumDesks);
            assertThat(schools.size()).isEqualTo(numSchools/2);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of schools that have classrooms with no desks
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.numberDesks", noDesks);
            assertThat(schools.size()).isEqualTo(1);
            readWriteTxn.commit();
        }

        // Get by secondary index, retrieve number of schools that have classrooms with computers (repeated primitive field)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.School, ManagedMetadata>> schools = readWriteTxn
                    .getByIndex(table, "classRooms.classInfra.others", "computers");
            assertThat(schools.size()).isEqualTo(numSchools/others.length);
            readWriteTxn.commit();
        }

        log.debug(table.getMetrics().toString());
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
                Uuid.class,
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
                Uuid.class,
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
                Uuid.class,
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
        Table<Uuid, ExampleSchemas.Adult, ManagedMetadata> adultsTable = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ExampleSchemas.Adult.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        ManagedMetadata user = ManagedMetadata.newBuilder().setCreateUser("user_UT").build();

        for (int i = 0; i < adultCount; i++) {
            UUID adultId = UUID.randomUUID();
            Uuid adultKey = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "age", adultBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (using fully qualified name), retrieve from database all adults with adultsBaseAge
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "person.age", adultBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age 'kidsBaseAge'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "kidsAge", kidsBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (fully qualified name), retrieve from database all adults with kids on age 'kidsBaseAge'
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(adultsTable, "person.children.child.age", kidsBaseAge);
            assertThat(entries.size()).isEqualTo(adultCount/2);
            readWriteTxn.commit();
        }

        // Get by secondary index (custom alias), retrieve from database all adults with kids on age '2' (non existent)
        try (ManagedTxnContext readWriteTxn = shimStore.txn(someNamespace)) {
            List<CorfuStoreEntry<Uuid, ExampleSchemas.Adult, ManagedMetadata>> entries = readWriteTxn
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
                Uuid.class,
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
                Uuid.class,
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
        Table<Uuid, ExampleSchemas.NotNestedSecondaryIndex, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ExampleSchemas.NotNestedSecondaryIndex.class,
                ManagedMetadata.class,
                TableOptions.builder().build());

        UUID id = UUID.randomUUID();
        Uuid key1 = Uuid.newBuilder()
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
            List<CorfuStoreEntry<Uuid, ExampleSchemas.NotNestedSecondaryIndex, ManagedMetadata>> entries = readWriteTxn
                    .getByIndex(table, "field3", 0L);
            assertThat(entries.size()).isEqualTo(1);
            assertThat(entries.get(0).getPayload().getField1()).isEqualTo("record_1");
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
        CorfuRuntime corfuRuntime = getTestRuntime();

        // Creating Corfu Store using a connected corfu client.
        CorfuStoreShim shimStore = new CorfuStoreShim(corfuRuntime);

        // Define a namespace for the table.
        final String someNamespace = "some-namespace";
        // Define table name.
        final String tableName = "ManagedMetadata";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder()
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

        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
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
        Table<Uuid, ManagedMetadata, ManagedMetadata> table =
                shimStore.openTable(
                        someNamespace,
                        tableName,
                        Uuid.class,
                        ManagedMetadata.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        Uuid key1 = Uuid.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
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
        Table<Uuid, ManagedMetadata, Messages.LogReplicationEntryMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                Messages.LogReplicationEntryMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();
        final String something = "double_nested_metadata_field";
        final int one = 1; // Frankly stupid but i could not figure out how to selectively disable checkstyle
        final long twelve = 12L; // please help figure out how to disable checkstyle selectively

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    Messages.LogReplicationEntryMetadata.newBuilder()
                            .setSiteConfigID(twelve)
                            .setSyncRequestId(Uuid.newBuilder().setMsb(one).build())
                            .build());
            txn.commit();
        }

        // Update the record, validate that metadata fields not set, get merged with existing
        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value,
                    Messages.LogReplicationEntryMetadata.newBuilder()
                            .setTimestamp(one+twelve)
                            .build());
            txn.commit();
        }
        CorfuStoreEntry<Uuid, ManagedMetadata, Messages.LogReplicationEntryMetadata> entry = null;
        try (ManagedTxnContext queryTxn = shimStore.tx(someNamespace)) {
            entry = queryTxn.getRecord(table, key);
        }

        assertThat(entry.getMetadata().getSiteConfigID()).isEqualTo(twelve);
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
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        try (ManagedTxnContext txn = shimStore.tx(someNamespace)) {
            txn.putRecord(tableName, key, value); // Look no metadata specified!
            txn.commit();
        }

        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
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
        Table<Uuid, ManagedMetadata, ManagedMetadata> table = shimStore.openTable(
                someNamespace,
                tableName,
                Uuid.class,
                ManagedMetadata.class,
                ManagedMetadata.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        Uuid key = Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        ManagedMetadata value = ManagedMetadata.newBuilder().setCreateUser("simpleValue").build();

        class NestedTxnTester {
            public void nestedQuery() {
                CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
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
        CorfuStoreEntry<Uuid, ManagedMetadata, ManagedMetadata> entry;
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
        Table<Messages.Uuid, Messages.Uuid, Message> conflictTable =
                shimStore.openTable(
                        someNamespace,
                        conflictTableName,
                        Messages.Uuid.class,
                        Messages.Uuid.class,
                        null,
                        // TableOptions includes option to choose - Memory/Disk based corfu table.
                        TableOptions.builder().build());

        Messages.Uuid key = Messages.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
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
                Messages.Uuid conflictKey = Messages.Uuid.newBuilder().setMsb(coinToss).build();
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
}
