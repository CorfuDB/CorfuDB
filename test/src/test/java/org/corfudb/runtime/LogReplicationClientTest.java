package org.corfudb.runtime;

import com.google.common.collect.Iterables;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.LogReplication.ClientInfo;
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

        final Table<StringKey, ClientInfo, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                StringKey.class,
                ClientInfo.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, clientName);

        // Test registering a new client
        Assert.assertTrue(client.registerReplicationClient());

        // Test registering an existing client
        Assert.assertFalse(client.registerReplicationClient());

        // Test registering additional clients
        LogReplicationClient client2 = new LogReplicationClient(runtime, "client2");
        LogReplicationClient client3 = new LogReplicationClient(runtime, "client3");
        Assert.assertTrue(client2.registerReplicationClient());
        Assert.assertTrue(client3.registerReplicationClient());

        // Test that the number of clients in the table is correct
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
        Assert.assertTrue(client.addDestination("DOMAIN", "DESTINATION1"));
        Assert.assertTrue(client.addDestination("DOMAIN", "DESTINATION2"));

        // Test adding existing destinations
        Assert.assertFalse(client.addDestination("DOMAIN", "DESTINATION2"));

        // Test adding a destination with an invalid domain
        Assert.assertFalse(client.addDestination("", "DESTINATION3"));
        Assert.assertFalse(client.addDestination(null, "DESTINATION4"));

        // Test adding a destination with an invalid name
        Assert.assertFalse(client.addDestination("DOMAIN", ""));
        Assert.assertFalse(client.addDestination("DOMAIN", (String) null));

        // Test that the table size is correct
        Assert.assertEquals(1, table.count());

        // Test that the number of destinations in the domain are correct
        // DOMAIN1 has 2 destinations after the above, same with DOMAIN2 after the below
        Assert.assertTrue(client.addDestination("DOMAIN2", "DESTINATION1"));
        Assert.assertTrue(client.addDestination("DOMAIN2", "DESTINATION2"));

        final int batchSize = 2;
        final int numDestinations = 2;

        // Test that the table size has increased with the addition of DOMAIN2
        Assert.assertEquals(2, table.count());

        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                Assert.assertEquals(numDestinations, entry.getPayload().getDestinationIdsList().size());
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

        // Test adding destinations to a domain
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination("DOMAIN", destinationsToAdd));
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination("DOMAIN", destinationsToAdd2));

        // Test adding existing destinations
        List<String> destinationsToAdd3 = Arrays.asList("DESTINATION1", "DESTINATION4");
        Assert.assertFalse(client.addDestination("DOMAIN", destinationsToAdd3));

        // Test adding with request having a malformed domain
        List<String> destinationsToAdd4 = Collections.singletonList("DESTINATION1");
        Assert.assertFalse(client.addDestination("", destinationsToAdd4));
        Assert.assertFalse(client.addDestination(null, destinationsToAdd4));

        // Test adding with request having a malformed destination
        List<String> destinationsToAdd5 = Collections.singletonList("");
        List<String> destinationsToAdd6 = Collections.singletonList(null);
        List<String> destinationsToAdd7 = Arrays.asList("DESTINATION1", "DESTINATION1");
        Assert.assertFalse(client.addDestination("DOMAIN", destinationsToAdd5));
        Assert.assertFalse(client.addDestination("DOMAIN", destinationsToAdd6));
        Assert.assertFalse(client.addDestination("DOMAIN", destinationsToAdd7));

        // Test adding destinations to new domain
        List<String> destinationsToAdd8 = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination("DOMAIN2", destinationsToAdd8));

        // Test addition after removal in new domain
        Assert.assertTrue(client.removeDestination("DOMAIN2", "DESTINATION2"));
        List<String> destinationsToAdd9 = Arrays.asList("DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination("DOMAIN2", destinationsToAdd9));

        // Test table size is correct
        Assert.assertEquals(2, table.count());

        final int batchSize = 2;
        final int[] numDestination = {4, 3};
        int currentDestination = 0;

        // Test if final list of destinations are as expected for each domain
        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                Assert.assertEquals(numDestination[currentDestination], entry.getPayload().getDestinationIdsList().size());
                currentDestination++;
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
        final String domain = "DOMAIN";
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
        Assert.assertFalse(client.removeDestination(domain, destination));

        // Test adding a destination
        Assert.assertTrue(client.addDestination(domain, destination));

        // Test removing the added destination
        Assert.assertTrue(client.removeDestination(domain, destination));

        // Test removing the destination again after it has been removed
        Assert.assertFalse(client.removeDestination(domain, destination));

        // Test table size is correct
        Assert.assertEquals(1, table.count());

        final int batchSize = 1;
        final int numDestinations = 0;

        // Test if final list of destinations are as expected for each domain
        Stream<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> entry : partition) {
                Assert.assertEquals(numDestinations, entry.getPayload().getDestinationIdsList().size());
            }
        }
    }
}
