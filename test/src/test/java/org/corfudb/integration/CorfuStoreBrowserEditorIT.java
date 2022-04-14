package org.corfudb.integration;

import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.browser.CorfuStoreBrowserEditor;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleAppliance;
import org.corfudb.test.SampleSchema;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class CorfuStoreBrowserEditorIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws
        IOException {
        return new AbstractIT.CorfuServerRunner()
            .setHost(host)
            .setPort(port)
            .setLogPath(getCorfuServerLogPath(host, port))
            .setSingle(true)
            .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty(
            "corfuSingleNodePort"));
        singleNodeEndpoint = String.format(
            "%s:%d",
            corfuSingleNodeHost,
            corfuStringNodePort
        );
    }

    /**
     * Test print metadata map functionality of Browser
     *
     * @throws Exception
     */
    @Test
    public void testPrintMetadataMap() throws Exception {
        Process corfuServer = null;
        try {
            corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
            final String namespace = "namespace";
            final String tableName = "table";
            final int totalUpdates = 5;
            List<CorfuStoreMetadata.Timestamp> committedTimestamps = new ArrayList();

            // Start a Corfu runtime & Corfu Store
            runtime = createRuntime(singleNodeEndpoint);
            CorfuStore store = new CorfuStore(runtime);

            // Open one table and write couple of updates
            final Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.ManagedMetadata> tableA = store.openTable(
                    namespace,
                    tableName,
                    SampleSchema.Uuid.class,
                    SampleSchema.SampleTableAMsg.class,
                    SampleSchema.ManagedMetadata.class,
                    TableOptions.builder().build());

            for(int i = 0; i < totalUpdates; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleSchema.SampleTableAMsg value = SampleSchema.SampleTableAMsg.newBuilder().setPayload(Integer.toString(i)).build();
                SampleSchema.ManagedMetadata metadata = SampleSchema.ManagedMetadata.newBuilder().setCreateTime(System.currentTimeMillis())
                        .setCreateUser("User_" + i).build();
                try (TxnContext tx = store.txn(namespace)) {
                    tx.putRecord(tableA, key, value, metadata);
                    committedTimestamps.add(tx.commit());
                }
            }

            // Create CorfuStoreBrowser on its own dedicated runtime
            CorfuRuntime browserRuntime = createRuntime(singleNodeEndpoint);
            CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(browserRuntime);

            committedTimestamps.forEach(ts -> {
                EnumMap<IMetadata.LogUnitMetadataType, Object> metadataMap = browser.printMetadataMap(ts.getSequence());
                // TODO: fix tx.commit() returning wrong epoch (txSnapshot)
                // assertThat(ts.getEpoch()).isEqualTo(metadataMap.get(IMetadata.LogUnitMetadataType.EPOCH));
                assertThat(0L).isEqualTo(metadataMap.get(IMetadata.LogUnitMetadataType.EPOCH));
                assertThat(ts.getSequence()).isEqualTo(metadataMap.get(IMetadata.LogUnitMetadataType.GLOBAL_ADDRESS));
                assertThat(Thread.currentThread().getId()).isEqualTo(metadataMap.get(IMetadata.LogUnitMetadataType.THREAD_ID)); });
        } finally {
            if (corfuServer != null) {
                shutdownCorfuServer(corfuServer);
            }
        }
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void browserTest() throws
        IOException,
        NoSuchMethodException,
        IllegalAccessException,
        InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
            corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
            .setMsb(keyUuid)
            .setLsb(keyUuid)
            .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
            .setMsb(valueUuid)
            .setLsb(valueUuid)
            .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
            .setMsb(metadataUuid)
            .setLsb(metadataUuid)
            .build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        final int one = 1;
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
        Assert.assertEquals(browser.listTables(namespace), one);

        // Invoke the browser and go through each item
        CorfuTable table = browser.getTable(namespace, tableName);
        Assert.assertEquals(browser.printTable(namespace, tableName), one);
        for(Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(namespace, tableName), one);
        // Invoke dropTable and verify size
        Assert.assertEquals(browser.clearTable(namespace, tableName), one);
        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(namespace, tableName), 0);
    }

    /**
     * Create a table and add data to it using the loadTable command.
     * @throws IOException
     */
    @Test
    public void loaderTest() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);
        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);
        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
                .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
                .setRuleId(ruleIdVal).setRuleName("Test Rule")
                .setInput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
                .setOutput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
                .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
                .setMsb(metaUuid).build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();
        runtime.shutdown();

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);
        final int numItems = 100;
        final int batchSize = 10;
        final int itemSize = 100;

        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        Assert.assertEquals(browser.loadTable(namespace, tableName, numItems, batchSize, itemSize), batchSize);
        runtime.shutdown();
    }

    /**
     * Test Corfu Browser stream tags APIs (tag list & tags to table names mapping)
     *
     * @throws Exception
     */
    @Test
    public void testBrowserTagsOperations() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        final String namespace = "UT-namespace";
        final String tableBaseName = "table";

        Map<String, List<String>> expectedTableNameToTags = populateRegistryTable(namespace, tableBaseName);
        Map<String, List<String>> expectedTagToTableNames = new HashMap<>();
        expectedTableNameToTags.forEach((tName, tags) -> {
            tags.forEach(tag -> {
                if (expectedTagToTableNames.containsKey(tag)) {
                    expectedTagToTableNames.computeIfPresent(tag, (key, tNames) -> {
                        tNames.add(tName);
                        return tNames;
                    });
                } else {
                    List<String> listTableNames = new ArrayList<>();
                    listTableNames.add(tName);
                    expectedTagToTableNames.put(tag, listTableNames);
                }
            });
        });

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);

        // (1) List Stream Tags
        Set<String> tagsInRegistry = browser.listStreamTags();
        assertThat(tagsInRegistry.size()).isEqualTo(expectedTagToTableNames.keySet().size());
        assertThat(tagsInRegistry).containsOnly(expectedTagToTableNames.keySet().toArray(new String[0]));

        // (2) Show Stream Tag Maps (tags to table names)
        Map<String, List<CorfuStoreMetadata.TableName>> tagToTableNames = browser.listTagToTableMap();
        assertThat(tagToTableNames.size()).isEqualTo(expectedTagToTableNames.keySet().size());
        assertThat(tagToTableNames.keySet()).containsOnly(expectedTagToTableNames.keySet().toArray(new String[0]));
        tagToTableNames.forEach((tag, tableNames) -> assertThat(tableNames.size()).isEqualTo(expectedTagToTableNames.get(tag).size()));

        // (3) List Tables for a given stream tag
        final String streamTag = "sample_streamer_2";
        List<CorfuStoreMetadata.TableName> tablesForStreamTag = browser.listTablesForTag(streamTag);
        assertThat(tablesForStreamTag.size()).isEqualTo(expectedTagToTableNames.get(streamTag).size());
        tablesForStreamTag.forEach(table -> assertThat(expectedTagToTableNames.get(streamTag)).contains(table.getTableName()));

        // (4) List tags for a given table
        final String tableName = tableBaseName + 0; // Pick first created table which corresponds to SampleTableAMsg Schema (2 tags)
        Set<String> tags = browser.listTagsForTable(namespace, tableName);
        assertThat(tags.size()).isEqualTo(expectedTableNameToTags.get(tableName).size());
        assertThat(tags).containsExactly(expectedTableNameToTags.get(tableName).toArray(new String[0]));

        runtime.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private Map<String, List<String>> populateRegistryTable(String namespace, String tableBaseName) throws Exception {
        // Start a Corfu runtime & CorfuStore
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        Map<String, List<String>> tableNameToTags = new HashMap<>();

        // Create 12 tables, each with different combinations among 4 different tags (some with no tags).
        // Tags are determined by the value types (refer to sample_schema.proto for defined tags of each type)
        final int totalTables = 12;
        // Refer to sample_schema.proto
        List<Class> valueTypes = Arrays.asList(SampleSchema.SampleTableAMsg.class, SampleSchema.SampleTableBMsg.class,
                SampleSchema.SampleTableCMsg.class, SampleSchema.SampleTableDMsg.class);
        Map<Class, List<String>> expectedTagsPerValues =  new HashMap<>();
        expectedTagsPerValues.put(SampleSchema.SampleTableAMsg.class, Arrays.asList("sample_streamer_1", "sample_streamer_2"));
        expectedTagsPerValues.put(SampleSchema.SampleTableBMsg.class, Arrays.asList("sample_streamer_2", "sample_streamer_3"));
        expectedTagsPerValues.put(SampleSchema.SampleTableCMsg.class, Collections.EMPTY_LIST);
        expectedTagsPerValues.put(SampleSchema.SampleTableDMsg.class, Arrays.asList("sample_streamer_4"));

        for (int index = 0; index < totalTables; index++) {
            store.openTable(namespace, tableBaseName + index,
                    SampleSchema.Uuid.class, valueTypes.get(index % valueTypes.size()), SampleSchema.Uuid.class,
                    TableOptions.fromProtoSchema(valueTypes.get(index % valueTypes.size())));
            tableNameToTags.put(tableBaseName + index, expectedTagsPerValues.get(valueTypes.get(index % valueTypes.size())));
        }

        runtime.shutdown();
        return tableNameToTags;
    }

    /**
     * Test Corfu Browser protobuf descriptor table
     *
     * @throws Exception
     */
    @Test
    public void testListAllProtos() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        final String namespace = "UT-namespace";
        final String tableBaseName = "table";

        final int expectedFiles = 5;
        populateRegistryTable(namespace, tableBaseName);

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        assertThat(browser.printAllProtoDescriptors()).isEqualTo(expectedFiles);

        runtime.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Create a table and add nested protobufs as data to it.  Verify that the
     * browser tool is able to read the contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void nestedProtoTest() throws
        IOException,
        NoSuchMethodException,
        IllegalAccessException,
        InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
            .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
            .setRuleId(ruleIdVal).setRuleName("Test Rule")
            .setInput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .setOutput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
            .setMsb(metaUuid).build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        CorfuTable table2 = browser.getTable(namespace, tableName);
        browser.printTable(namespace, tableName);
        Assert.assertEquals(1, table2.size());

        for(Object obj : table2.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
        runtime.shutdown();
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read the system TableRegistry contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void browserRegistryTableTest() throws
            IOException,
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                null,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table, uuidKey, uuidVal, null);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
        final int three = 3;
        Assert.assertEquals(three,
            browser.printTableInfo(TableRegistry.CORFU_SYSTEM_NAMESPACE,
        TableRegistry.REGISTRY_TABLE_NAME));
        Assert.assertEquals(1, browser.printTableInfo(namespace, tableName));
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read disk based tables in disk based mode.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void browserDiskBasedTableTest() throws
            IOException,
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class,
                        TableOptions.builder()
                                .persistentDataPath(Paths.get(PARAMETERS.TEST_TEMP_DIR)).build())
        );

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
                .setMsb(metadataUuid)
                .setLsb(metadataUuid)
                .build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table, uuidKey, uuidVal, metadata);
        tx.commit();

        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        String tempDir = com.google.common.io.Files.createTempDir()
                .getAbsolutePath();
        final CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime, tempDir);
        // Verify table count
        Assert.assertEquals(1, browser.printTable(namespace, tableName));

        runtime.shutdown();
    }

    @Test
    public void editorTest() throws IOException, NoSuchMethodException,
        IllegalAccessException, InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);
        ProtobufSerializer protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
        runtime.getSerializers().registerSerializer(protoSerializer);

        CorfuStore store = new CorfuStore(runtime);

        Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
            namespace,
            tableName,
            SampleSchema.Uuid.class,
            SampleSchema.Uuid.class,
            SampleSchema.Uuid.class,
            TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
            .setMsb(keyUuid)
            .setLsb(keyUuid)
            .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
            .setMsb(valueUuid)
            .setLsb(valueUuid)
            .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
            .setMsb(metadataUuid)
            .setLsb(metadataUuid)
            .build();
        TxnContext tx = store.txn(namespace);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
//        runtime.shutdown();
//
//        runtime = createRuntime(singleNodeEndpoint);
//        protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
//        runtime.getSerializers().registerSerializer(protoSerializer);

        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
//        Assert.assertEquals(browser.listTables(namespace), 1);


//        int numEntriesBefore = browser.printTable(namespace, tableName);

//        assertThat(table1.count()).isEqualTo(1);
        browser.radioTest(namespace, tableName, 10, 1, 10);

        assertThat(table1.count()).isEqualTo(11);


        runtime.shutdown();
    }
}
