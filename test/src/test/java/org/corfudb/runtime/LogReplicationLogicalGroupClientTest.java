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
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogReplicationLogicalGroupClientTest extends AbstractIT {

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
        String corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        int corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        String singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);

        namespace = "CorfuSystem";
        registrationTableName = "LogReplicationRegistrationTable";
        metadataTableName = "LogReplicationModelMetadataTable";
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
        new LogReplicationLogicalGroupClient(runtime, clientName);
        final int expectedNumberRegisteredClients = 1;
        Assert.assertEquals(expectedNumberRegisteredClients, table.count());

        // Test registering a duplicate client, table count should be the same
        new LogReplicationLogicalGroupClient(runtime, clientName);
        final int expectedNumberRegisteredClients1 = 1;
        Assert.assertEquals(expectedNumberRegisteredClients1, table.count());

        // Test registering additional clients
        new LogReplicationLogicalGroupClient(runtime, "client2");
        new LogReplicationLogicalGroupClient(runtime, "client3");
        final int expectedNumberRegisteredClients2 = 3;
        Assert.assertEquals(expectedNumberRegisteredClients2, table.count());
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

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding new destinations
        client.addDestination("LOGICALGROUP1", "DESTINATION1");
        client.addDestination("LOGICALGROUP1", "DESTINATION2");

        // Test adding existing destinations
        final int expectedNumberDestinations = 2;
        final int currentTableEntry = 0;
        client.addDestination("LOGICALGROUP1", "DESTINATION2");
        Assert.assertEquals(expectedNumberDestinations, table.entryStream().collect(Collectors.toList()).get(currentTableEntry).getPayload().getDestinationIdsList().size());

        // Test adding a destination with an invalid logicalGroup
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("", "DESTINATION3"));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination(null, "DESTINATION4"));

        // Test adding a destination with an invalid name
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICALGROUP1", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICALGROUP1", (String) null));

        // Test that the table size is correct
        final int expectedNumberRegisteredGroups = 1;
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());

        // Test adding destinations to a second logical group
        client.addDestination("LOGICALGROUP2", "DESTINATION1");
        client.addDestination("LOGICALGROUP2", "DESTINATION2");

        // Test that the table size has increased with the addition of LOGICALGROUP2
        final int expectedNumberRegisteredGroups1 = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups1, table.count());

        final int batchSize = 2;
        final int numDestinationsGroup1 = 2;
        final int numDestinationsGroup2 = 2;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put("LOGICALGROUP1", numDestinationsGroup1);
        numDestinations.put("LOGICALGROUP2", numDestinationsGroup2);

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

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding destinations to a logicalGroup
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        client.addDestination("LOGICALGROUP1", destinationsToAdd);
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION3", "DESTINATION4");
        client.addDestination("LOGICALGROUP1", destinationsToAdd2);

        // Test adding existing destinations
        final int expectedNumberDestinations = 4;
        final int currentTableEntry = 0;
        List<String> destinationsToAdd3 = Arrays.asList("DESTINATION1", "DESTINATION4");
        client.addDestination("LOGICALGROUP1", destinationsToAdd3);
        Assert.assertEquals(expectedNumberDestinations, table.entryStream().collect(Collectors.toList()).get(currentTableEntry).getPayload().getDestinationIdsList().size());

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToAdd4 = Collections.singletonList("DESTINATION1");
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("", destinationsToAdd4));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination(null, destinationsToAdd4));

        // Test adding with request having a malformed destination
        List<String> destinationsToAdd5 = Collections.singletonList("");
        List<String> destinationsToAdd6 = Collections.singletonList(null);
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICALGROUP1", destinationsToAdd5));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICALGROUP1", destinationsToAdd6));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICALGROUP1", (List<String>) null));

        // Test adding destinations to new logicalGroup
        List<String> destinationsToAdd8 = Arrays.asList("DESTINATION1", "DESTINATION2");
        client.addDestination("LOGICALGROUP2", destinationsToAdd8);

        // Test addition after removal in new logicalGroup
        client.removeDestination("LOGICALGROUP2", "DESTINATION2");
        List<String> destinationsToAdd9 = Arrays.asList("DESTINATION3", "DESTINATION4");
        client.addDestination("LOGICALGROUP2", destinationsToAdd9);

        // Test table size is correct
        final int expectedNumberRegisteredGroups1 = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups1, table.count());

        final int batchSize = 2;
        final int numDestinationsGroup1 = 4;
        final int numDestinationsGroup2 = 3;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put("LOGICALGROUP1", numDestinationsGroup1);
        numDestinations.put("LOGICALGROUP2", numDestinationsGroup2);

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
        final String logicalGroup = "LOGICALGROUP1";
        final String destination = "DESTINATION";

        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test group is removed after no destinations left
        final int expectedNumberRegisteredGroups = 0;
        client.addDestination(logicalGroup, "INIT");
        client.removeDestination(logicalGroup, "INIT");
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());

        // Test removing a destination that does not exist
        final int currentTableEntry = 0;
        client.addDestination(logicalGroup, "TEMPDEST");
        final int sizeBeforeAttemptedDelete = table.entryStream().collect(Collectors.toList()).get(currentTableEntry).getPayload().getDestinationIdsList().size();
        client.removeDestination(logicalGroup, destination);
        Assert.assertEquals(sizeBeforeAttemptedDelete, table.entryStream().collect(Collectors.toList()).get(currentTableEntry).getPayload().getDestinationIdsList().size());
        client.removeDestination(logicalGroup, "TEMPDEST");

        // Test removing a logicalGroup that does not exist
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination("NOTAGROUP", destination));

        // Test adding a destination
        client.addDestination(logicalGroup, destination);

        // Test removing the added destination
        client.removeDestination(logicalGroup, destination);

        // Test removing the destination again after it has been removed
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination(logicalGroup, destination));

        // Test table size is correct
        final int expectedNumberRegisteredGroups1 = 0;
        Assert.assertEquals(expectedNumberRegisteredGroups1, table.count());

        final int batchSize = 1;
        final int numDestinationsGroup1 = 0;
        final HashMap<String, Integer> numDestinations = new HashMap<>();
        numDestinations.put(logicalGroup, numDestinationsGroup1);

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
        final String logicalGroup = "LOGICALGROUP1";
        final String destination = "DESTINATION";

        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test group is removed after no destinations left
        final int expectedNumberRegisteredGroups = 0;
        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION2");
        client.addDestination(logicalGroup, destinationsToAdd);
        client.removeDestination(logicalGroup, destinationsToAdd);
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());

        // Test removing a destination that does not exist
        List<String> destinationsToRemove = Collections.singletonList(destination);
        client.addDestination(logicalGroup, "INIT");
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination(logicalGroup, destinationsToRemove));
        client.removeDestination(logicalGroup, "INIT");

        // Test removing from a logicalGroup that does not exist
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination("NOTAGROUP", destination));

        // Test removals of list that is a subset
        final int expectedNumberDestinations = 1;
        final int currentTableEntry = 0;
        List<String> destinationsToAdd2 = Arrays.asList("DESTINATION1", "DESTINATION2", "DESTINATION3");
        List<String> destinationsToRemove2 = Arrays.asList("DESTINATION1", "DESTINATION2");
        client.addDestination(logicalGroup, destinationsToAdd2);
        client.removeDestination(logicalGroup, destinationsToRemove2);
        Assert.assertEquals(expectedNumberDestinations, table.entryStream().collect(Collectors.toList()).get(currentTableEntry).getPayload().getDestinationIdsList().size());

        // Test removal of a list that is a superset
        final int expectedNumberRegisteredGroups1 = 0;
        List<String> destinationsToAdd3 = Arrays.asList("DESTINATION2", "DESTINATION3");
        List<String> destinationsToRemove3 = Arrays.asList("DESTINATION1", "DESTINATION2", "DESTINATION3", "DESTINATION4");
        client.addDestination(logicalGroup, destinationsToAdd3);
        client.removeDestination(logicalGroup, destinationsToRemove3);
        Assert.assertEquals(expectedNumberRegisteredGroups1, table.count());

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToRemove4 = Collections.singletonList("DESTINATION1");
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("", destinationsToRemove4));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(null, destinationsToRemove4));

        // Test adding with request having a malformed destination
        List<String> destinationsToRemove7 = Collections.singletonList("");
        List<String> destinationsToRemove8 = Collections.singletonList(null);
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(logicalGroup, destinationsToRemove7));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(logicalGroup, destinationsToRemove8));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(logicalGroup, (List<String>) null));
    }

    /**
     * Test show destinations for a logical group
     *
     * @throws Exception exception
     */
    @Test
    public void testShowDestinations() throws Exception {
        final String logicalGroup = "LOGICALGROUP";
        final String destination1 = "DESTINATION1";
        final String destination2 = "DESTINATION2";

        final Table<ClientDestinationInfoKey, DestinationInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination(logicalGroup, ""));

        client.addDestination(logicalGroup, destination1);
        client.addDestination(logicalGroup, destination2);

        client.addDestination(logicalGroup, Arrays.asList("D1", "D2", "D1"));
        final String invalidDestination = "INVALID";
        Assert.assertThrows(NoSuchElementException.class, () -> client.showDestinations(invalidDestination));

        client .showDestinations(logicalGroup);
    }
}
