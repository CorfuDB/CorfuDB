package org.corfudb.runtime;

import com.google.common.collect.Iterables;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.LogReplication.LRClientId;
import org.corfudb.runtime.LogReplication.LRClientInfo;
import org.corfudb.runtime.LogReplication.ClientInfoKey;
import org.corfudb.runtime.LogReplication.SinksInfoVal;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class LogReplicationClientTest extends AbstractIT {
    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private void runSinglePersistentServer(String host, int port) throws IOException {
        new CorfuServerRunner()
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
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * Test registering replication client
     *
     * @throws Exception exception
     */
    @Test
    public void testRegisterReplicationClient() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationRegistrationTable";
        final String clientName = "client";

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<LRClientId, LRClientInfo, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                LRClientId.class,
                LRClientInfo.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        // Test registering a new client
        new LogReplicationClient(runtime, clientName);
        Assert.assertEquals(1, table.count());

        // Test registering a duplicate client, table count should be the same
        new LogReplicationClient(runtime, clientName);
        Assert.assertEquals(1, table.count());

        // Test registering additional clients
        new LogReplicationClient(runtime, "client2");
        new LogReplicationClient(runtime, "client3");
        Assert.assertEquals(3, table.count());
    }

    /**
     * Test add destination
     *
     * @throws Exception exception
     */
    @Test
    public void testAddDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String clientName = "client";

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, clientName);

        // Test adding new destinations
        Assert.assertTrue(client.addDestination("LOGICALGROUP", "DESTINATION1"));
        Assert.assertTrue(client.addDestination("LOGICALGROUP", "DESTINATION2"));

        // Test adding existing destinations
        Assert.assertFalse(client.addDestination("LOGICALGROUP", "DESTINATION2"));

        // Test adding a destination with an invalid logicalGroup
        Assert.assertFalse(client.addDestination("", "DESTINATION3"));
        Assert.assertFalse(client.addDestination(null, "DESTINATION4"));

        // Test adding a destination with an invalid name
        Assert.assertFalse(client.addDestination("LOGICALGROUP", ""));
        Assert.assertFalse(client.addDestination("LOGICALGROUP", (String) null));

        // Test that the table size is correct
        Assert.assertEquals(1, table.count());

        // Test that the number of destinations in the logicalGroup are correct
        // LOGICALGROUP has 2 destinations after the above, same with LOGICALGROUP2 after the below
        Assert.assertTrue(client.addDestination("LOGICALGROUP2", "DESTINATION1"));
        Assert.assertTrue(client.addDestination("LOGICALGROUP2", "DESTINATION2"));

        // Test that the table size has increased with the addition of LOGICALGROUP2
        Assert.assertEquals(2, table.count());

        final int batchSize = 2;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put("LOGICALGROUP", 2);
        numDestinations.put("LOGICALGROUP2", 2);

        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                String currentGroup = entry.getKey().getGroupName();
                Integer currentNumberDestinations = entry.getPayload().getDestinationIdsList().size();
                Assert.assertEquals(numDestinations.get(currentGroup), currentNumberDestinations);
            }
        }
    }

    /**
     * Test add multiple destinations
     *
     * @throws Exception exception
     */
    @Test
    public void testAddListOfDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String clientName = "client";

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, clientName);

        // Test adding destinations to a logicalGroup
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination("LOGICALGROUP", destinationsToAdd));
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination("LOGICALGROUP", destinationsToAdd2));

        // Test adding existing destinations
        List<String> destinationsToAdd3 = Arrays.asList("DESTINATION1", "DESTINATION4");
        Assert.assertFalse(client.addDestination("LOGICALGROUP", destinationsToAdd3));

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToAdd4 = Collections.singletonList("DESTINATION1");
        Assert.assertFalse(client.addDestination("", destinationsToAdd4));
        Assert.assertFalse(client.addDestination(null, destinationsToAdd4));

        // Test adding with request having a malformed destination
        List<String> destinationsToAdd5 = Collections.singletonList("");
        List<String> destinationsToAdd6 = Collections.singletonList(null);
        List<String> destinationsToAdd7 = Arrays.asList("DESTINATION1", "DESTINATION1");
        Assert.assertFalse(client.addDestination("LOGICALGROUP", destinationsToAdd5));
        Assert.assertFalse(client.addDestination("LOGICALGROUP", destinationsToAdd6));
        Assert.assertFalse(client.addDestination("LOGICALGROUP", destinationsToAdd7));

        // Test adding destinations to new logicalGroup
        List<String> destinationsToAdd8 = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination("LOGICALGROUP2", destinationsToAdd8));

        // Test addition after removal in new logicalGroup
        Assert.assertTrue(client.removeDestination("LOGICALGROUP2", "DESTINATION2"));
        List<String> destinationsToAdd9 = Arrays.asList("DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination("LOGICALGROUP2", destinationsToAdd9));

        // Test table size is correct
        Assert.assertEquals(2, table.count());

        final int batchSize = 2;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put("LOGICALGROUP", 4);
        numDestinations.put("LOGICALGROUP2", 3);

        // Test if final list of destinations are as expected for each logicalGroup
        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                String currentGroup = entry.getKey().getGroupName();
                Integer currentNumberDestinations = entry.getPayload().getDestinationIdsList().size();
                Assert.assertEquals(numDestinations.get(currentGroup), currentNumberDestinations);
            }
        }
    }

    /**
     * Test remove destination
     *
     * @throws Exception exception
     */
    @Test
    public void testRemoveDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String clientName = "client";
        final String logicalGroup = "LOGICALGROUP";
        final String destination = "DESTINATION";

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, clientName);

        // Test removing a destination that does not exist
        Assert.assertFalse(client.removeDestination(logicalGroup, destination));

        // Test adding a destination
        Assert.assertTrue(client.addDestination(logicalGroup, destination));

        // Test removing the added destination
        Assert.assertTrue(client.removeDestination(logicalGroup, destination));

        // Test removing the destination again after it has been removed
        Assert.assertFalse(client.removeDestination(logicalGroup, destination));

        // Test table size is correct
        Assert.assertEquals(1, table.count());

        final int batchSize = 1;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put(logicalGroup, 0);

        // Test if final list of destinations are as expected for each logicalGroup
        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                String currentGroup = entry.getKey().getGroupName();
                Integer currentNumberDestinations = entry.getPayload().getDestinationIdsList().size();
                Assert.assertEquals(numDestinations.get(currentGroup), currentNumberDestinations);
            }
        }
    }
}
