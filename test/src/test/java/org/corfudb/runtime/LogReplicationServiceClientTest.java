package org.corfudb.runtime;

import com.google.common.collect.Iterables;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogReplicationServiceClientTest extends AbstractIT {
    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    private static String namespace;

    private static String registrationTableName;

    private static String metadataTableName;

    private static String clientName;

    private static CorfuRuntime runtime;

    private static CorfuStore store;

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
    public void loadProperties() throws Exception {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);

        namespace = "CorfuSystem";
        registrationTableName = "LogReplicationRegistrationTable";
        metadataTableName = "LogReplicationSourceMetadataTable";
        clientName = "client";

        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        runtime = createRuntime(singleNodeEndpoint);
        store = new CorfuStore(runtime);
    }

    /**
     * Test registering replication client
     *
     * @throws Exception exception
     */
    @Test
    public void testRegisterReplicationClient() throws Exception {
        final Table<ClientRegistrationId, ClientRegistrationInfo, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                registrationTableName,
                ClientRegistrationId.class,
                ClientRegistrationInfo.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        // Test registering a new client
        new LogReplicationServiceClient(runtime, clientName);
        Assert.assertEquals(1, table.count());

        // Test registering a duplicate client, table count should be the same
        new LogReplicationServiceClient(runtime, clientName);
        Assert.assertEquals(1, table.count());

        // Test registering additional clients
        new LogReplicationServiceClient(runtime, "client2");
        new LogReplicationServiceClient(runtime, "client3");
        Assert.assertEquals(3, table.count());
    }

    /**
     * Test add destination
     *
     * @throws Exception exception
     */
    @Test
    public void testAddDestination() throws Exception {
        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationServiceClient client = new LogReplicationServiceClient(runtime, clientName);

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

        Stream<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> entry : partition) {
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
        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationServiceClient client = new LogReplicationServiceClient(runtime, clientName);

        // Test adding destinations to a logicalGroup
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination("LOGICALGROUP", destinationsToAdd));
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination("LOGICALGROUP", destinationsToAdd2));

        // Test adding existing destinations
        List<String> destinationsToAdd3 = Arrays.asList("DESTINATION1", "DESTINATION4");
        Assert.assertTrue(client.addDestination("LOGICALGROUP", destinationsToAdd3));

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
        Assert.assertFalse(client.addDestination("LOGICALGROUP", (List<String>) null));

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
        Stream<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> entry : partition) {
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
        final String logicalGroup = "LOGICALGROUP";
        final String destination = "DESTINATION";

        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationServiceClient client = new LogReplicationServiceClient(runtime, clientName);

        // Add and remove dummy destination to initialize the logicalGroup
        Assert.assertTrue(client.addDestination(logicalGroup, "INIT"));
        Assert.assertTrue(client.removeDestination(logicalGroup, "INIT"));

        // Test removing a destination that does not exist
        Assert.assertFalse(client.removeDestination(logicalGroup, destination));

        // Test removing from a logicalGroup that does not exist
        Assert.assertFalse(client.removeDestination("NOTAGROUP", destination));

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
        Stream<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> entry : partition) {
                String currentGroup = entry.getKey().getGroupName();
                Integer currentNumberDestinations = entry.getPayload().getDestinationIdsList().size();
                Assert.assertEquals(numDestinations.get(currentGroup), currentNumberDestinations);
            }
        }
    }

    /**
     * Test removal of multiple destinations
     *
     * @throws Exception exception
     */
    @Test
    public void testRemoveListOfDestinations() throws Exception {
        final String logicalGroup = "LOGICALGROUP";
        final String destination = "DESTINATION";

        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationServiceClient client = new LogReplicationServiceClient(runtime, clientName);

        // Add and remove dummy destination to initialize the logicalGroup
        Assert.assertTrue(client.addDestination(logicalGroup, "INIT"));
        Assert.assertTrue(client.removeDestination(logicalGroup, "INIT"));

        // Test removing a destination that does not exist
        List<String> destinationsToRemove = Collections.singletonList(destination);
        Assert.assertFalse(client.removeDestination(logicalGroup, destinationsToRemove));

        // Test removing from a logicalGroup that does not exist
        Assert.assertFalse(client.removeDestination("NOTAGROUP", destination));

        // Test removal of destinations
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        List<String> destinationsToRemove2 = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination(logicalGroup, destinationsToAdd));
        Assert.assertTrue(client.removeDestination(logicalGroup, destinationsToRemove2));

        // Test removals of list that is a subset
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION1", "DESTINATION2", "DESTINATION3");
        List<String> destinationsToRemove3 = Arrays.asList("DESTINATION1", "DESTINATION2");
        Assert.assertTrue(client.addDestination(logicalGroup, destinationsToAdd2));
        Assert.assertTrue(client.removeDestination(logicalGroup, destinationsToRemove3));
        Assert.assertEquals(1, table.entryStream().collect(Collectors.toList()).get(0).getPayload().getDestinationIdsList().size());

        // Test removal of a list that is a superset
        List<String> destinationsToAdd4 = Arrays.asList("DESTINATION2", "DESTINATION3");
        List<String> destinationsToRemove5 = Arrays.asList("DESTINATION1", "DESTINATION2", "DESTINATION3", "DESTINATION4");
        Assert.assertTrue(client.addDestination(logicalGroup, destinationsToAdd4));
        Assert.assertTrue(client.removeDestination(logicalGroup, destinationsToRemove5));
        Assert.assertEquals(0, table.entryStream().collect(Collectors.toList()).get(0).getPayload().getDestinationIdsList().size());

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToRemove6 = Collections.singletonList("DESTINATION1");
        Assert.assertFalse(client.removeDestination("", destinationsToRemove6));
        Assert.assertFalse(client.removeDestination(null, destinationsToRemove6));

        // Test adding with request having a malformed destination
        List<String> destinationsToRemove7 = Collections.singletonList("");
        List<String> destinationsToRemove8 = Collections.singletonList(null);
        List<String> destinationsToRemove9 = Arrays.asList("DESTINATION1", "DESTINATION1");
        Assert.assertFalse(client.removeDestination(logicalGroup, destinationsToRemove7));
        Assert.assertFalse(client.removeDestination(logicalGroup, destinationsToRemove8));
        Assert.assertFalse(client.removeDestination(logicalGroup, destinationsToRemove9));
        Assert.assertFalse(client.removeDestination(logicalGroup, (List<String>) null));

        // Test table size is correct
        Assert.assertEquals(1, table.count());

        final int batchSize = 1;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put(logicalGroup, 0);

        // Test if final list of destinations are as expected for each logicalGroup
        Stream<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> entry : partition) {
                String currentGroup = entry.getKey().getGroupName();
                Integer currentNumberDestinations = entry.getPayload().getDestinationIdsList().size();
                Assert.assertEquals(numDestinations.get(currentGroup), currentNumberDestinations);
            }
        }
    }
}
