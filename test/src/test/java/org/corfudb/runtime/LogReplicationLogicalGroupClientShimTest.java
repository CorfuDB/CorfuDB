package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.collections.CorfuStoreShim;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema.ManagedResources;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class LogReplicationLogicalGroupClientShimTest extends AbstractViewTest {

    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
    }
    private static String namespace;
    private static String registrationTableName;
    private static String metadataTableName;
    private static String clientName;
    private static CorfuRuntime runtime;
    private static CorfuStoreShim store;

    @Before
    public void loadProperties() throws Exception {
        namespace = "CorfuSystem";
        registrationTableName = "LogReplicationRegistrationTable";
        metadataTableName = "LogReplicationModelMetadataTable";
        clientName = "client";

        runtime = getTestRuntime();
        store = new CorfuStoreShim(runtime);
    }

    /**
     * Test registering replication client
     *
     */
    @Test
    public void testRegisterReplicationClient() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        final Table<ClientRegistrationId, ClientRegistrationInfo, ManagedResources> table = store.openTable(
                namespace,
                registrationTableName,
                ClientRegistrationId.class,
                ClientRegistrationInfo.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        // Test registering client with null/empty client name
        Assert.assertThrows(IllegalArgumentException.class, () ->  new LogReplicationLogicalGroupClient(runtime, null));
        Assert.assertThrows(IllegalArgumentException.class, () ->  new LogReplicationLogicalGroupClient(runtime, ""));

        // Test registering a new client
        new LogReplicationLogicalGroupClient(runtime, clientName);
        final int expectedNumberRegisteredClients = 1;
        Assert.assertEquals(expectedNumberRegisteredClients, table.count());

        // Test registering a duplicate client
        new LogReplicationLogicalGroupClient(runtime, clientName);
        final int expectedNumberRegisteredClients1 = 1;
        Assert.assertEquals(expectedNumberRegisteredClients1, table.count());

        // Test registering 2 additional clients
        new LogReplicationLogicalGroupClient(runtime, "client1");
        new LogReplicationLogicalGroupClient(runtime, "client2");
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
        final Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding a destination with null/empty logicalGroup
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("", "DESTINATION"));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination(null, "DESTINATION"));

        // Test adding a destination with null/empty name
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", (String) null));

        // Test adding a destination
        final int expectedNumberDestinations = 1;
        final String currentTableEntryKey = "LOGICAL-GROUP";
        client.addDestination("LOGICAL-GROUP", "DESTINATION");
        Assert.assertEquals(expectedNumberDestinations, table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test adding a duplicate of an existing destination, duplicate should not be added and log warning
        final int expectedNumberDestinations1 = 1;
        final String currentTableEntryKey1 = "LOGICAL-GROUP";
        client.addDestination("LOGICAL-GROUP", "DESTINATION");
        Assert.assertEquals(expectedNumberDestinations1, table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey1))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test adding destinations to a second logical group
        final int expectedNumberDestinations2 = 2;
        final String currentTableEntryKey2 = "LOGICAL-GROUP1";
        client.addDestination("LOGICAL-GROUP1", "DESTINATION");
        client.addDestination("LOGICAL-GROUP1", "DESTINATION1");
        Assert.assertEquals(expectedNumberDestinations2, table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey2))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test that the overall table size is correct, 2 expected since 2 groups created
        final int expectedNumberRegisteredGroups = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());
    }

    /**
     * Test remove destination
     *
     * @throws Exception exception
     */
    @Test
    public void testRemoveDestination() throws Exception {
        final Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding a destination with null/empty logicalGroup
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("", "DESTINATION"));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(null, "DESTINATION"));

        // Test adding a destination with null/empty name
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", (String) null));

        // Test removal from a group that does not exist
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination("LOGICAL-GROUP", "DESTINATION"));

        // Test add then remove, group should be deleted once empty
        final int expectedNumberRegisteredGroups = 0;
        client.addDestination("LOGICAL-GROUP", "DESTINATION");
        client.removeDestination("LOGICAL-GROUP", "DESTINATION");
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());

        // Test removal of destinations
        Set<String> expectedDestinations = new HashSet<>(Arrays.asList("DESTINATION", "DESTINATION2"));
        final String currentTableEntryKey = "LOGICAL-GROUP";
        client.addDestination("LOGICAL-GROUP", "DESTINATION");
        client.addDestination("LOGICAL-GROUP", "DESTINATION1");
        client.addDestination("LOGICAL-GROUP", "DESTINATION2");
        client.removeDestination("LOGICAL-GROUP", "DESTINATION1");
        // DESTINATION1 does not exist after removal, log warning here but should not throw exception
        client.removeDestination("LOGICAL-GROUP", "DESTINATION1");
        Assert.assertEquals(expectedDestinations, new HashSet<>(table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                .findFirst().get().getPayload().getDestinationIdsList()));
    }

    /**
     * Test add multiple destinations
     *
     * @throws Exception exception
     */
    @Test
    public void testAddListOfDestination() throws Exception {
        final Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToAdd = Collections.singletonList("DESTINATION");
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("", destinationsToAdd));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination(null, destinationsToAdd));

        // Test adding with request having a malformed destination
        List<String> destinationsToAdd1 = Collections.singletonList("");
        List<String> destinationsToAdd2 = Collections.singletonList(null);
        List<String> destinationsToAdd3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", destinationsToAdd1));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", destinationsToAdd2));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", destinationsToAdd3));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.addDestination("LOGICAL-GROUP", (List<String>) null));

        // Test adding list of destinations that contain a duplicate, list gets de-duplicated and log warning here
        final int expectedNumberDestinations = 1;
        final String currentTableEntryKey = "LOGICAL-GROUP";
        List<String> destinationsToAdd4 = Arrays.asList("DESTINATION", "DESTINATION");
        client.addDestination("LOGICAL-GROUP", destinationsToAdd4);
        Assert.assertEquals(expectedNumberDestinations, table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test adding destinations to a second logical group
        final int expectedNumberDestinations2 = 4;
        final String currentTableEntryKey2 = "LOGICAL-GROUP1";
        List<String> destinationsToAdd5 = Arrays.asList("DESTINATION", "DESTINATION1");
        List<String> destinationsToAdd6 = Arrays.asList("DESTINATION2", "DESTINATION3");
        client.addDestination("LOGICAL-GROUP1", destinationsToAdd5);
        client.addDestination("LOGICAL-GROUP1", destinationsToAdd6);
        Assert.assertEquals(expectedNumberDestinations2, table.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey2))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test that the overall table size is correct
        final int expectedNumberRegisteredGroups = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());
    }

    /**
     * Test removal of multiple destinations
     *
     * @throws Exception exception
     */
    @Test
    public void testRemoveListOfDestinations() throws Exception {
        final Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> table = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test adding with request having a malformed logicalGroup
        List<String> destinationsToRemove = Collections.singletonList("DESTINATION");
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("", destinationsToRemove));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination(null, destinationsToRemove));

        // Test adding with request having a malformed destination
        List<String> destinationsToRemove1 = Collections.singletonList("");
        List<String> destinationsToRemove2 = Collections.singletonList(null);
        List<String> destinationsToRemove3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", destinationsToRemove1));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", destinationsToRemove2));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", destinationsToRemove3));
        Assert.assertThrows(IllegalArgumentException.class, () -> client.removeDestination("LOGICAL-GROUP", (List<String>) null));

        // Test removal from a group that does not exist
        Assert.assertThrows(NoSuchElementException.class, () -> client.removeDestination("LOGICAL-GROUP", "DESTINATION"));

        // Test add then remove, group should be deleted once empty
        final int expectedNumberRegisteredGroups = 0;
        List<String> destinationsToRemove4 = Arrays.asList("DESTINATION", "DESTINATION1");
        client.addDestination("LOGICAL-GROUP", destinationsToRemove4);
        client.removeDestination("LOGICAL-GROUP", destinationsToRemove4);
        Assert.assertEquals(expectedNumberRegisteredGroups, table.count());

        // Test removal of multiple destinations
        Set<String> expectedDestinations = new HashSet<>(Arrays.asList("DESTINATION", "DESTINATION2"));
        final String currentTableEntryKey = "LOGICAL-GROUP";
        List<String> destinationsToAdd = Arrays.asList("DESTINATION", "DESTINATION1", "DESTINATION2", "DESTINATION3");
        List<String> destinationsToRemove5 = Arrays.asList("DESTINATION1", "DESTINATION3", "DESTINATION4", "DESTINATION5");
        client.addDestination("LOGICAL-GROUP", destinationsToAdd);
        // Some in destinationsToRemove5 do not exist, log warning here but should not throw exception
        client.removeDestination("LOGICAL-GROUP", destinationsToRemove5);
        // None in destinationsToRemove5 should exist after removal, log warning here but should not throw exception
        client.removeDestination("LOGICAL-GROUP", destinationsToRemove5);
        Assert.assertEquals(expectedDestinations, new HashSet<>(table.entryStream()
                        .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                        .findFirst().get().getPayload().getDestinationIdsList()));
    }

    /**
     * Test show destinations for a logical group
     *
     * @throws Exception exception
     */
    @Test
    public void testShowDestinations() throws Exception {
        store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        LogReplicationLogicalGroupClient client = new LogReplicationLogicalGroupClient(runtime, clientName);

        // Test show destinations with null/empty group
        Assert.assertThrows(IllegalArgumentException.class, () ->  client.showDestinations(null));
        Assert.assertThrows(IllegalArgumentException.class, () ->  client.showDestinations(""));

        // Add destinations to show
        client.addDestination("LOGICAL-GROUP", "DESTINATION");
        client.addDestination("LOGICAL-GROUP", "DESTINATION1");

        // Test show destinations on an invalid group
        Assert.assertThrows(NoSuchElementException.class, () -> client.showDestinations("LOGICAL-GROUP1"));

        // Test show destinations
        client.showDestinations("LOGICAL-GROUP");
    }
}
