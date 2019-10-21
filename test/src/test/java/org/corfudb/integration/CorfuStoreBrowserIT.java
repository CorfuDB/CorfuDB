package org.corfudb.integration;

import com.google.protobuf.UnknownFieldSet;

import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.corfudb.browser.CorfuStoreBrowser;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.test.SampleAppliance;
import org.corfudb.test.SampleSchema;


public class CorfuStoreBrowserIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

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
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately.
     */
    @Test
    public void browserTest() throws Exception{
        final String namespace = "namespace";
        final String tableName = "table";
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
            corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        store.openTable(
            namespace,
            tableName,
            Uuid.class,
            Uuid.class,
            Uuid.class,
            TableOptions.builder().build());

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        Uuid uuidKey = Uuid.newBuilder()
            .setMsb(keyUuid)
            .setLsb(keyUuid)
            .build();
        Uuid uuidVal = Uuid.newBuilder()
            .setMsb(valueUuid)
            .setLsb(valueUuid)
            .build();
        Uuid metadata = Uuid.newBuilder()
            .setMsb(metadataUuid)
            .setLsb(metadataUuid)
            .build();
        TxBuilder tx = store.tx(namespace);
        tx.create(tableName, uuidKey, uuidVal, metadata)
            .update(tableName, uuidKey, uuidVal, metadata)
            .commit();
        runtime.shutdown();

        // Invoke the browser and go through each item
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowser browser = new CorfuStoreBrowser(runtime);
        CorfuTable table = browser.getTable(namespace, tableName);
        browser.printTable(table);
        Assert.assertEquals(1, table.size());
        for(Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
    }

    /**
     * Create a table and add nested protobufs as data to it.  Verify that the
     * browser tool is able to read the contents accurately.
     */
    @Test
    public void nestedProtoTest() throws Exception{
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        store.openTable(
            namespace,
            tableName,
            Uuid.class,
            SampleSchema.FirewallRule.class,
            Uuid.class,
            TableOptions.builder().build());

        Uuid uuidKey = Uuid.newBuilder().setLsb(keyUuid)
            .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
            .setRuleId(ruleIdVal).setRuleName("Test Rule")
            .setInput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .setOutput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .build();
        Uuid uuidMeta = Uuid.newBuilder().setLsb(metaUuid)
            .setMsb(metaUuid).build();
        TxBuilder tx = store.tx(namespace);
        tx.create(tableName, uuidKey, firewallRuleVal, uuidMeta)
            .update(tableName, uuidKey, firewallRuleVal, uuidMeta)
            .commit();
        runtime.shutdown();

        runtime = createRuntime(singleNodeEndpoint);
        CorfuStoreBrowser browser = new CorfuStoreBrowser(runtime);
        CorfuTable table = browser.getTable(namespace, tableName);
        browser.printTable(table);
        Assert.assertEquals(1, table.size());

        for(Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
    }
}
