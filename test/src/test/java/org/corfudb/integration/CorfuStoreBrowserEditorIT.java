package org.corfudb.integration;

import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import org.corfudb.browser.CorfuOfflineBrowserEditor;
import org.corfudb.browser.CorfuStoreBrowserEditor;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleAppliance;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.SampleTableAMsg;
import org.corfudb.test.SampleSchema.SampleTableBMsg;
import org.corfudb.test.SampleSchema.SampleTableCMsg;
import org.corfudb.test.SampleSchema.SampleTableDMsg;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
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
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:magicnumber")
public class CorfuStoreBrowserEditorIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    private String logPath;

    private static final String TEST_RULE = "Test Rule";
    private static final String NAMESPACE = "namespace";
    private static final String TABLE_NAME = "table";
    private static final String LOCALHOST = "localhost";

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws
        IOException {
        logPath = getCorfuServerLogPath(host, port);
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
        corfuStringNodePort = Integer.parseInt(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format(
            "%s:%d",
            corfuSingleNodeHost,
            corfuStringNodePort
        );
    }

    /**
     * Test print metadata map functionality of Browser
     *
     * @throws Exception error
     */
    @Test
    public void testPrintMetadataMap() throws Exception {
        Process corfuServer = null;
        try {
            corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
            final int totalUpdates = 5;
            List<CorfuStoreMetadata.Timestamp> committedTimestamps = new ArrayList<>();

            // Start a Corfu runtime & Corfu Store
            CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
            CorfuStore store = new CorfuStore(runtime);

            // Open one table and write a couple of updates
            final Table<SampleSchema.Uuid, SampleTableAMsg, SampleSchema.ManagedMetadata> tableA = store.openTable(
                    NAMESPACE,
                    TABLE_NAME,
                    SampleSchema.Uuid.class,
                    SampleTableAMsg.class,
                    SampleSchema.ManagedMetadata.class,
                    TableOptions.builder().build());

            for(int i = 0; i < totalUpdates; i++) {
                SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
                SampleTableAMsg value = SampleTableAMsg.newBuilder().setPayload(Integer.toString(i)).build();
                SampleSchema.ManagedMetadata metadata = SampleSchema.ManagedMetadata.newBuilder().setCreateTime(System.currentTimeMillis())
                        .setCreateUser("User_" + i).build();
                try (TxnContext tx = store.txn(NAMESPACE)) {
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
     * @throws Exception error
     */
    @Test
    public void browserTest() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
            corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        final int one = 1;
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
        Assert.assertEquals(browser.listTables(NAMESPACE), one);

        // Invoke the browser and go through each item
        ICorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = browser.getTable(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(browser.printTable(NAMESPACE, TABLE_NAME), one);
        for(Object obj : table.entryStream().map(Map.Entry::getValue).collect(Collectors.toList())) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), one);
        // Invoke dropTable and verify size
        Assert.assertEquals(browser.clearTable(NAMESPACE, TABLE_NAME), one);
        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), 0);
    }

    /**
     * Create a table and add data to it using the loadTable command.
     * @throws IOException error
     */
    @Test
    public void loaderTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);
        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
                .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
                .setRuleId(ruleIdVal).setRuleName(TEST_RULE)
                .setInput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .setOutput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
                .setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();
        runtime.shutdown();

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);
        final int numItems = 100;
        final int batchSize = 10;
        final int itemSize = 100;

        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        Assert.assertEquals(browser.loadTable(NAMESPACE, TABLE_NAME, numItems, batchSize, itemSize), batchSize);
        runtime.shutdown();
    }

    /**
     * Test Corfu Browser stream tags APIs (tag list & tags to table names mapping)
     *
     * @throws Exception error
     */
    @Test
    public void testBrowserTagsOperations() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        Map<String, List<String>> expectedTableNameToTags = populateRegistryTable(NAMESPACE, TABLE_NAME);
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

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
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
        final String tableName = TABLE_NAME + 0; // Pick first created table which corresponds to SampleTableAMsg Schema (2 tags)
        Set<String> tags = browser.listTagsForTable(NAMESPACE, tableName);
        assertThat(tags.size()).isEqualTo(expectedTableNameToTags.get(tableName).size());
        assertThat(tags).containsExactly(expectedTableNameToTags.get(tableName).toArray(new String[0]));

        runtime.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    private Map<String, List<String>> populateRegistryTable(String namespace, String tableBaseName) throws Exception {
        // Start a Corfu runtime & CorfuStore
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        Map<String, List<String>> tableNameToTags = new HashMap<>();

        // Create 12 tables, each with different combinations among 4 different tags (some with no tags).
        // Tags are determined by the value types (refer to sample_schema.proto for defined tags of each type)
        final int totalTables = 12;
        // Refer to sample_schema.proto
        List<Class<? extends com.google.protobuf.Message>> valueTypes = Arrays.asList(
                SampleTableAMsg.class,
                SampleTableBMsg.class,
                SampleTableCMsg.class,
                SampleTableDMsg.class
        );

        Map<Class<? extends com.google.protobuf.Message>, List<String>> expectedTagsPerValues =  new HashMap<>();
        expectedTagsPerValues.put(SampleTableAMsg.class, Arrays.asList("sample_streamer_1", "sample_streamer_2"));
        expectedTagsPerValues.put(SampleTableBMsg.class, Arrays.asList("sample_streamer_2", "sample_streamer_3"));
        expectedTagsPerValues.put(SampleTableCMsg.class, Collections.emptyList());
        expectedTagsPerValues.put(SampleTableDMsg.class, Arrays.asList("sample_streamer_4"));

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
     * @throws Exception error
     */
    @Test
    public void testListAllProtos() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        final int expectedFiles = 5;
        populateRegistryTable(NAMESPACE, TABLE_NAME);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        assertThat(browser.printAllProtoDescriptors()).isEqualTo(expectedFiles);

        runtime.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Test Corfu Offline Browser protobuf descriptor table
     *
     * @throws Exception error
     */
    @Test
    public void testListAllProtoOffline() throws Exception {
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
                corfuStringNodePort);

        final int expectedFiles = 6;
        populateRegistryTable(NAMESPACE, TABLE_NAME);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        assertThat(browser.printAllProtoDescriptors()).isEqualTo(expectedFiles);

        runtime.shutdown();

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Create a table and add nested protobufs as data to it.  Verify that the
     * browser tool is able to read the contents accurately.
     * @throws Exception error
     */
    @Test
    public void nestedProtoTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
            .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
            .setRuleId(ruleIdVal).setRuleName(TEST_RULE)
            .setInput(
                SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
            .setOutput(
                SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
            .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
            .setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        ICorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table2 = browser.getTable(NAMESPACE, TABLE_NAME);
        browser.printTable(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(1, table2.size());

        for(Object obj : table2.entryStream().map(Map.Entry::getValue).collect(Collectors.toList())) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
        runtime.shutdown();
    }

    /**
     * Create a table and add nested protobufs as data to it. Verify that the
     * offline browser tool is able to read the contents accurately.
     * @throws Exception error
     */
    @Test
    public void nestedProtoTestOfflineBrowser() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
                .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
                .setRuleId(ruleIdVal).setRuleName(TEST_RULE)
                .setInput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .setOutput(
                        SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
                .setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();
        runtime.shutdown();

        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> tableData = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(tableData.size(), one);

        for (CorfuDynamicKey key: tableData.keySet()) {
            Assert.assertEquals(key.getKey().toString(), uuidKey.toString());
            Assert.assertEquals(tableData.get(key).getPayload().toString(), firewallRuleVal.toString());
            Assert.assertEquals(tableData.get(key).getMetadata().toString(), uuidMeta.toString());
        }
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read the system TableRegistry contents accurately.
     * @throws Exception error
     */
    @Test
    public void browserRegistryTableTest() throws Exception{
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
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
        Assert.assertEquals(1, browser.printTableInfo(NAMESPACE, TABLE_NAME));
    }

    /**
     * Create a table and add data to it.  Verify that the offline browser is able
     * to read the system TableRegistry contents accurately.
     * @throws Exception error
     */
    @Test
    public void browserRegistryTableTestOffline() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                null,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final long keyUuid = 1L;
        final long valueUuid = 3L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, uuidVal, null);
        tx.commit();
        runtime.shutdown();


        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        // Invoke listTables and verify table count
        final int three = 3;
        final int one = 1;
        Assert.assertEquals(browser.printTableInfo(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME), three);
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), one);
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read disk based tables in disk based mode.
     * @throws Exception error
     */
    @Test
    public void browserDiskBasedTableTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class,
                        TableOptions.builder()
                                .persistentDataPath(Paths.get(PARAMETERS.TEST_TEMP_DIR)).build())
        );

        assertThat(table.getUnderlyingType()).isEqualTo(PersistedCorfuTable.class);

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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, uuidVal, metadata);
        tx.commit();

        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        String tempDir = com.google.common.io.Files.createTempDir()
                .getAbsolutePath();
        final CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime, tempDir);
        // Verify table count
        Assert.assertEquals(1, browser.printTable(NAMESPACE, TABLE_NAME));

        runtime.shutdown();
    }

    @Test
    public void editorTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
            NAMESPACE,
            TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
        Assert.assertEquals(browser.listTables(NAMESPACE), 1);

        // Edit the record changing value from 3L -> 5L
        String keyString = "{\"msb\": \"1\", \"lsb\": \"1\"}";
        String newValString = "{\"msb\": \"5\", \"lsb\": \"5\"}";
        final long newVal = 5L;
        SampleSchema.Uuid newValUuid = SampleSchema.Uuid.newBuilder()
            .setMsb(newVal)
            .setLsb(newVal)
            .build();

        CorfuDynamicRecord editedRecord = browser.editRecord(NAMESPACE,
            TABLE_NAME, keyString, newValString);
        Assert.assertNotNull(editedRecord);

        DynamicMessage dynamicValMessage = DynamicMessage.newBuilder(newValUuid)
            .build();
        String valTypeUrl = Any.pack(newValUuid).getTypeUrl();
        DynamicMessage dynamicMetadataMessage = DynamicMessage.newBuilder(metadata)
            .build();
        String metadataTypeUrl = Any.pack(metadata).getTypeUrl();
        CorfuDynamicRecord expectedRecord = new CorfuDynamicRecord(valTypeUrl,
            dynamicValMessage, metadataTypeUrl, dynamicMetadataMessage);

        Assert.assertEquals(expectedRecord, editedRecord);

        final int batchSize = 10000;
        // Now test deleteRecord capability
        assertThat(browser.deleteRecords(NAMESPACE, TABLE_NAME, Arrays.asList(keyString), batchSize)).isEqualTo(1);
        // Try to edit the deleted key and verify it is a no-op
        Assert.assertNull(browser.editRecord(NAMESPACE, TABLE_NAME, keyString,
            newValString));
        // Try to delete a deleted key and verify it is a no-op
        assertThat(browser.deleteRecords(NAMESPACE, TABLE_NAME, Arrays.asList(keyString), batchSize)).isZero();
        runtime.shutdown();
    }

    /**
     * Put all the records to be deleted in a file and test the batched deletion capability
     *
     * @throws Exception error
     */
    @Test
    public void batchedDeletionTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final int numRecords = PARAMETERS.NUM_ITERATIONS_MODERATE;
        List<String> recordsAsJson = new ArrayList<>(numRecords);
        try (TxnContext tx = store.txn(NAMESPACE)) {
            for (int i = 0; i < numRecords; i++) {
                SampleSchema.Uuid simpleRecord = SampleSchema.Uuid.newBuilder()
                        .setMsb(i)
                        .setLsb(0)
                        .build();
                tx.putRecord(table1, simpleRecord, simpleRecord, simpleRecord);
                StringBuilder keyBuilder = new StringBuilder();
                keyBuilder.append("{\"msb\": \"")
                        .append(i)
                        .append("\", \"lsb\": \"0\"}");
                recordsAsJson.add(keyBuilder.toString());
            }
            tx.commit();
        }
        // Now also write all the records out to a file
        final String pathToRecordsToDelete = CORFU_LOG_PATH + File.separator + "recordsToDelete";
        FileWriter writer = new FileWriter(pathToRecordsToDelete);
        for (String jsonRecord: recordsAsJson) {
            writer.write(jsonRecord + System.lineSeparator());
        }
        writer.close();

        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);

        int deletedRecordCount = browser.deleteRecordsFromFile(NAMESPACE, TABLE_NAME,
                pathToRecordsToDelete, numRecords / 10);
        assertThat(deletedRecordCount).isEqualTo(numRecords);
        runtime.shutdown();
    }

    @Test
    public void addRecordTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
            NAMESPACE,
            TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);
        // Invoke listTables and verify table count
        Assert.assertEquals(1, browser.printTable(NAMESPACE, TABLE_NAME));

        // Add a new record
        final String newKeyString = "{\"msb\": \"2\", \"lsb\": \"2\"}";
        final String newValString = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newMetadataString = "{\"msb\": \"6\", \"lsb\": \"6\"}";
        final long newVal = 4L;
        SampleSchema.Uuid newValUuid = SampleSchema.Uuid.newBuilder()
            .setMsb(newVal)
            .setLsb(newVal)
            .build();

        final long metadataVal = 6L;
        SampleSchema.Uuid newMetadataUuid = SampleSchema.Uuid.newBuilder()
            .setMsb(metadataVal)
            .setLsb(metadataVal)
            .build();

        CorfuDynamicRecord addedRecord = browser.addRecord(NAMESPACE,
            TABLE_NAME, newKeyString, newValString, newMetadataString);
        Assert.assertNotNull(addedRecord);

        DynamicMessage dynamicValMessage = DynamicMessage.newBuilder(newValUuid)
            .build();
        String valTypeUrl = Any.pack(newValUuid).getTypeUrl();
        DynamicMessage dynamicMetadataMessage =
            DynamicMessage.newBuilder(newMetadataUuid).build();
        String metadataTypeUrl = Any.pack(newMetadataUuid).getTypeUrl();
        CorfuDynamicRecord expectedRecord = new CorfuDynamicRecord(valTypeUrl,
            dynamicValMessage, metadataTypeUrl, dynamicMetadataMessage);

        Assert.assertEquals(expectedRecord, addedRecord);
        Assert.assertEquals(2, browser.printTable(NAMESPACE, TABLE_NAME));
    }

    /**
     * Verify that a record with null or empty key and/or value cannot be
     * inserted and a record with null or empty metadata can be inserted.
     */
    @Test
    public void addRecordTestWithNullAndEmpty() throws IOException,
        InvocationTargetException, NoSuchMethodException,
        IllegalAccessException {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
            NAMESPACE,
            TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);

        // Invoke printTable and verify table count
        Assert.assertEquals(1, browser.printTable(NAMESPACE, TABLE_NAME));

        // Add a new record with null key
        final String newKeyString1 = null;
        final String newValString1 = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newMetadataString1 = "{\"msb\": \"6\", \"lsb\": \"6\"}";

        CorfuDynamicRecord addedRecord = browser.addRecord(NAMESPACE,
            TABLE_NAME, newKeyString1, newValString1, newMetadataString1);

        // Verify that the record cannot be added
        Assert.assertNull(addedRecord);
        Assert.assertEquals(1, browser.printTable(NAMESPACE, TABLE_NAME));

        // Add a new record with empty value string
        final String newKeyString2 = "{\"msb\": \"2\", \"lsb\": \"2\"}";
        final String newValString2 = "";
        final String newMetadataString2 = newMetadataString1;

        addedRecord = browser.addRecord(NAMESPACE, TABLE_NAME, newKeyString2,
            newValString2, newMetadataString2);
        // Verify that the record cannot be added
        Assert.assertNull(addedRecord);
        Assert.assertEquals(1, browser.printTable(NAMESPACE, TABLE_NAME));


        // Add a new record with empty metadata and verify it can be added
        final String newKeyString3 = newKeyString2;
        final String newValString3 = newValString1;
        final String newMetadataString3 = "";

        final long newVal = 4L;
        SampleSchema.Uuid newValUuid = SampleSchema.Uuid.newBuilder()
            .setMsb(newVal)
            .setLsb(newVal)
            .build();

        SampleSchema.Uuid newMetadataUuid = SampleSchema.Uuid.newBuilder()
            .build();

        addedRecord = browser.addRecord(NAMESPACE,
            TABLE_NAME, newKeyString3, newValString3, newMetadataString3);
        Assert.assertNotNull(addedRecord);

        DynamicMessage dynamicValMessage = DynamicMessage.newBuilder(newValUuid)
            .build();
        String valTypeUrl = Any.pack(newValUuid).getTypeUrl();
        DynamicMessage dynamicMetadataMessage = null;
        String metadataTypeUrl = Any.pack(newMetadataUuid).getTypeUrl();
        CorfuDynamicRecord expectedRecord = new CorfuDynamicRecord(valTypeUrl,
            dynamicValMessage, metadataTypeUrl, dynamicMetadataMessage);

        Assert.assertEquals(expectedRecord, addedRecord);
        Assert.assertEquals(2, browser.printTable(NAMESPACE, TABLE_NAME));
    }

    @Test
    public void addRecordToATableWithoutMetadata() throws IOException,
            InvocationTargetException, NoSuchMethodException,
            IllegalAccessException {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                null,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final long keyUuid = 1L;
        final long valueUuid = 3L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid)
                .setLsb(keyUuid)
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid)
                .setLsb(valueUuid)
                .build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, null);
        tx.commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);

        // Add a new record with empty metadata and verify it can be added
        final String newKeyString1 = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newValString1 = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newMetadataString1 = "";

        CorfuDynamicRecord addedRecord = browser.addRecord(NAMESPACE,
                TABLE_NAME, newKeyString1, newValString1, newMetadataString1);
        Assert.assertNotNull(addedRecord);

        // Verify
        final long newVal = 4L;
        SampleSchema.Uuid newValUuid = SampleSchema.Uuid.newBuilder()
                .setMsb(newVal)
                .setLsb(newVal)
                .build();
        CorfuDynamicRecord expectedRecord = new CorfuDynamicRecord(
                Any.pack(SampleSchema.Uuid.getDefaultInstance()).getTypeUrl(),
                DynamicMessage.newBuilder(newValUuid).build(),
                "",
                null);

        Assert.assertEquals(expectedRecord, addedRecord);
        Assert.assertEquals(2, browser.printTable(NAMESPACE, TABLE_NAME));
    }

    /**
     * Verify that addRecord fails on a non-existent table.
     */
    @Test
    public void addRecordTestWithNonExistentTable() throws IOException {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStoreBrowserEditor browser = new CorfuStoreBrowserEditor(runtime);

        // Invoke printTable and verify table count
        Assert.assertEquals(0, browser.listTables(NAMESPACE));

        // New key, value, metadata to add
        final String newKeyString = "{\"msb\": \"2\", \"lsb\": \"2\"}";
        final String newValString = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newMetadataString = "{\"msb\": \"6\", \"lsb\": \"6\"}";

        // Adding the record must fail as the table does not exist.
        CorfuDynamicRecord addedRecord = browser.addRecord(NAMESPACE,
            TABLE_NAME, newKeyString, newValString, newMetadataString);
        Assert.assertNull(addedRecord);
    }

    /**
     * Open an offlineBrowser and test for adding an entry.
     * @throws Exception error
     */
    @Test
    public void addRecordOfflineBrowserTest() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        runtime.shutdown();

        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);

        // New key, value, metadata to add
        final String newKeyString = "{\"msb\": \"2\", \"lsb\": \"2\"}";
        final String newValString = "{\"msb\": \"4\", \"lsb\": \"4\"}";
        final String newMetadataString = "{\"msb\": \"6\", \"lsb\": \"6\"}";

        // Adding the record must fail as the table does not exist.
        CorfuDynamicRecord addedRecord = browser.addRecord(NAMESPACE,
                TABLE_NAME, newKeyString, newValString, newMetadataString);
        Assert.assertNull(addedRecord);
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately.
     * @throws Exception error
     */
    @Test
    public void offlineBrowserTest() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
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
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table1, uuidKey, uuidVal, metadata);
        tx.commit();
        runtime.shutdown();

        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);

        // Invoke listTables and verify table count
        Assert.assertEquals(browser.listTables(NAMESPACE), one);

        // Invoke the browser and go through each item
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> table = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(browser.printTable(NAMESPACE, TABLE_NAME), one);
        for(CorfuDynamicRecord obj : table.values()) {
            Assert.assertEquals(
                    UnknownFieldSet.newBuilder().build(),
                    obj.getPayload().getUnknownFields());
        }

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), one);
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately. Then delete the data and verify read again.
     * @throws Exception error
     */
    @Test
    public void writeDeleteTxnOfflineBrowser() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
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

        TxnContext tx1 = store.txn(NAMESPACE);
        tx1.putRecord(table1, uuidKey, uuidVal, metadata);
        tx1.commit();

        TxnContext tx2 = store.txn(NAMESPACE);
        tx2.delete(table1, uuidKey);
        tx2.commit();

        runtime.shutdown();

        final int zero = 0;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), zero);
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately. Then clear the table and verify read again.
     * @throws Exception error
     */
    @Test
    public void clearTableTxnOfflineBrowser() throws Exception {
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1 = store.openTable(
                NAMESPACE,
                TABLE_NAME,
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

        TxnContext tx1 = store.txn(NAMESPACE);
        tx1.putRecord(table1, uuidKey, uuidVal, metadata);
        tx1.commit();

        final int zero = 0;
        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), one);

        // Clear table1
        TxnContext tx2 = store.txn(NAMESPACE);
        tx2.clear(table1);
        tx2.commit();

        // Invoke tableInfo and verify size
        Assert.assertEquals(browser.printTableInfo(NAMESPACE, TABLE_NAME), zero);

        runtime.shutdown();
    }

    /**
     * Create a table and add data to it and update it.
     * Verify that the offline browser is able to
     * read its contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void readUpdatedTxnOfflineBrowser() throws
            IOException,
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long valUuid = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid).setMsb(keyUuid).build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder().setLsb(valUuid).setMsb(valUuid).build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid).setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, uuidVal, uuidMeta);
        tx.commit();

        final long updatedUuid = 50L;
        SampleSchema.Uuid uuidUpdated = SampleSchema.Uuid.newBuilder().setLsb(updatedUuid).setMsb(updatedUuid).build();
        TxnContext newtx = store.txn(NAMESPACE);
        newtx.putRecord(table, uuidKey, uuidUpdated, uuidMeta);
        newtx.commit();

        runtime.shutdown();
        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> tableData = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(tableData.size(), one);

        for (CorfuDynamicKey key: tableData.keySet()) {
            Assert.assertEquals(key.getKey().toString(), uuidKey.toString());
            Assert.assertEquals(tableData.get(key).getPayload().toString(), uuidUpdated.toString());
            Assert.assertEquals(tableData.get(key).getMetadata().toString(), uuidMeta.toString());
        }
    }

    /**
     * Create a table and add data to it and update it.
     * Trim the table and verify that the offline browser
     * is able to read its contents accurately.
     * @throws Exception err
     */
    @Test
    public void readUpdatedTxnWithTrimOfflineBrowser() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long valUuid = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid).setMsb(keyUuid).build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder().setLsb(valUuid).setMsb(valUuid).build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid).setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, uuidVal, uuidMeta);
        tx.commit();
        checkpointAndTrim(runtime, NAMESPACE, Arrays.asList(TABLE_NAME), false);

        final long updatedUuid = 50L;
        SampleSchema.Uuid uuidUpdated = SampleSchema.Uuid.newBuilder().setLsb(updatedUuid).setMsb(updatedUuid).build();
        TxnContext newtx = store.txn(NAMESPACE);
        newtx.putRecord(table, uuidKey, uuidUpdated, uuidMeta);
        newtx.commit();

        runtime.shutdown();
        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> tableData = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(tableData.size(), one);

        for (CorfuDynamicKey key: tableData.keySet()) {
            Assert.assertEquals(key.getKey().toString(), uuidKey.toString());
            Assert.assertEquals(tableData.get(key).getPayload().toString(), uuidUpdated.toString());
            Assert.assertEquals(tableData.get(key).getMetadata().toString(), uuidMeta.toString());
        }
    }

    /**
     * Create a table and add data to it.
     * Trim the table and verify that the offline browser
     * is able to read its contents accurately.
     * @throws Exception err
     */
    @Test
    public void trimStreamTestOffline() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid).setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
                .setRuleId(ruleIdVal).setRuleName(TEST_RULE)
                .setInput(SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .setOutput(SampleAppliance.Appliance.newBuilder().setEndpoint(LOCALHOST))
                .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid).setMsb(metaUuid).build();
        TxnContext tx = store.txn(NAMESPACE);
        tx.putRecord(table, uuidKey, firewallRuleVal, uuidMeta);
        tx.commit();

        checkpointAndTrim(runtime, NAMESPACE, Arrays.asList(TABLE_NAME), false);
        runtime.shutdown();

        final int one = 1;
        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> tableData = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(tableData.size(), one);

        for (CorfuDynamicKey key: tableData.keySet()) {
            Assert.assertEquals(key.getKey().toString(), uuidKey.toString());
            Assert.assertEquals(tableData.get(key).getPayload().toString(), firewallRuleVal.toString());
            Assert.assertEquals(tableData.get(key).getMetadata().toString(), uuidMeta.toString());
        }
    }

    /**
     * Create a table and add multiple entries to it.
     * Verify that the offline browser
     * is able to read its contents accurately.
     * @throws Exception err
     */
    @Test
    public void createMultipleEntriesOfflineBrowser() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                NAMESPACE,
                TABLE_NAME,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        final int numRecords = PARAMETERS.NUM_ITERATIONS_MODERATE;

        TxnContext tx = store.txn(NAMESPACE);
        for (int i = 0; i < numRecords; i++) {
            SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();
            SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(i).setMsb(i).build();

            tx.putRecord(table, uuidKey, uuidVal, uuidMeta);
        }
        tx.commit();

        runtime.shutdown();

        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> tableData = browser.getTableData(NAMESPACE, TABLE_NAME);
        Assert.assertEquals(tableData.size(), numRecords);
    }

    /**
     * Create a multiple tables and add multiple
     * entries to them. Verify that the offline browser
     * is able to read its contents accurately.
     * @throws Exception err
     */
    @Test
    public void createMultipleStreamAndEntriesOfflineBrowser() throws Exception {

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        final int numRecords = PARAMETERS.NUM_ITERATIONS_VERY_LOW;
        final int numTable = 8;
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        for (int i = 0; i < numTable; i++) {
            String namespace = Integer.toString(i);
            String tableName = Integer.toString(i);

            Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.openTable(
                    namespace,
                    tableName,
                    SampleSchema.Uuid.class,
                    SampleSchema.Uuid.class,
                    SampleSchema.Uuid.class,
                    TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

            TxnContext tx = store.txn(namespace);

            for (int j = 0; j < numRecords; j++) {
                SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(j).setMsb(j).build();
                SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder().setLsb(j).setMsb(j).build();
                SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(j).setMsb(j).build();

                tx.putRecord(table, uuidKey, uuidVal, uuidMeta);
            }
            tx.commit();
        }

        runtime.shutdown();

        CorfuOfflineBrowserEditor browser = new CorfuOfflineBrowserEditor(logPath);
        for (int k = 0; k < numTable; k++) {
            Assert.assertEquals(browser.printTableInfo(Integer.toString(k), Integer.toString(k)), numRecords);
        }
    }
}

