package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.ClientRegistrationId;
import org.corfudb.runtime.LogReplication.ClientRegistrationInfo;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.collections.CorfuStore;
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
import java.util.Set;

public class LogReplicationLogicalGroupClientTest extends AbstractViewTest {
    private static final String namespace = "CorfuSystem";
    private static final String registrationTableName = "LogReplicationRegistrationTable";
    private static final String metadataTableName = "LogReplicationModelMetadataTable";
    private static final String clientName = "client";

    private static Table<ClientRegistrationId, ClientRegistrationInfo, ManagedResources> replicationRegistrationTable;
    private static Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> sourceMetadataTable;
    private static CorfuRuntime runtime;
    private static LogReplicationLogicalGroupClient client;

    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
    }

    @Before
    public void loadProperties() throws Exception {
        runtime = getTestRuntime();
        CorfuStore store = new CorfuStore(runtime);

        replicationRegistrationTable = store.openTable(
                namespace,
                registrationTableName,
                ClientRegistrationId.class,
                ClientRegistrationInfo.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));
        sourceMetadataTable = store.openTable(
                namespace,
                metadataTableName,
                ClientDestinationInfoKey.class,
                DestinationInfoVal.class,
                ManagedResources.class,
                TableOptions.fromProtoSchema(Uuid.class));

        client = new LogReplicationLogicalGroupClient(runtime, clientName);
    }

    /**
     * Test registering replication client.
     *
     */
    @Test
    public void testRegisterReplicationClient()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        // Test registering client with null/empty client name.
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new LogReplicationLogicalGroupClient(runtime, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new LogReplicationLogicalGroupClient(runtime, ""));

        // Check to see if client was registered from the @Before function.
        final int expectedNumberRegisteredClients = 1;
        Assert.assertEquals(expectedNumberRegisteredClients, replicationRegistrationTable.count());

        // Test registering a duplicate client.
        new LogReplicationLogicalGroupClient(runtime, clientName);
        final int expectedNumberRegisteredClients1 = 1;
        Assert.assertEquals(expectedNumberRegisteredClients1, replicationRegistrationTable.count());

        // Test registering 2 additional clients.
        new LogReplicationLogicalGroupClient(runtime, "client1");
        new LogReplicationLogicalGroupClient(runtime, "client2");
        final int expectedNumberRegisteredClients2 = 3;
        Assert.assertEquals(expectedNumberRegisteredClients2, replicationRegistrationTable.count());
    }

    /**
     * Test adding multiple destinations.
     *
     */
    @Test
    public void testAddListOfDestinations() {
        // Test adding multiple destinations with request having a malformed logicalGroup.
        final List<String> destinationsToAdd = Collections.singletonList("DESTINATION");
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("", destinationsToAdd));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(null, destinationsToAdd));

        // Test adding multiple destinations with request having a malformed destination.
        final List<String> destinationsToAdd1 = Collections.singletonList("");
        final List<String> destinationsToAdd2 = Collections.singletonList(null);
        final List<String> destinationsToAdd3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("LOGICAL-GROUP", destinationsToAdd1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("LOGICAL-GROUP", destinationsToAdd2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("LOGICAL-GROUP", destinationsToAdd3));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("LOGICAL-GROUP", (List<String>) null));

        // Test adding list of destinations that contain a duplicate, list gets de-duplicated
        // and a warning is logged here.
        final int expectedNumberDestinations = 1;
        final String currentTableEntryKey = "LOGICAL-GROUP";
        final List<String> destinationsToAdd4 = Arrays.asList("DESTINATION", "DESTINATION");
        client.addDestinations("LOGICAL-GROUP", destinationsToAdd4);
        Assert.assertEquals(expectedNumberDestinations, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test adding destinations to a second logical group.
        final int expectedNumberDestinations2 = 4;
        final String currentTableEntryKey2 = "LOGICAL-GROUP1";
        final List<String> destinationsToAdd5 = Arrays.asList("DESTINATION", "DESTINATION1");
        final List<String> destinationsToAdd6 = Arrays.asList("DESTINATION2", "DESTINATION3");
        client.addDestinations("LOGICAL-GROUP1", destinationsToAdd5);
        // Test adding destinations that already exist, warning is logged here.
        client.addDestinations("LOGICAL-GROUP1", destinationsToAdd5);
        client.addDestinations("LOGICAL-GROUP1", destinationsToAdd6);
        Assert.assertEquals(expectedNumberDestinations2, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey2))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test that the overall table size is correct.
        final int expectedNumberRegisteredGroups = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());
    }

    /**
     * Test removing multiple destinations.
     *
     */
    @Test
    public void testRemoveListOfDestinations() {
        // Test removing multiple destinations with request having a malformed logicalGroup.
        final List<String> destinationsToRemove = Collections.singletonList("DESTINATION");
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("", destinationsToRemove));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(null, destinationsToRemove));

        // Test removing multiple destinations with request having a malformed destination.
        final List<String> destinationsToRemove1 = Collections.singletonList("");
        final List<String> destinationsToRemove2 = Collections.singletonList(null);
        final List<String> destinationsToRemove3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("LOGICAL-GROUP", destinationsToRemove1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("LOGICAL-GROUP", destinationsToRemove2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("LOGICAL-GROUP", destinationsToRemove3));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("LOGICAL-GROUP", (List<String>) null));

        // Test add then remove, group should be deleted once empty.
        final int expectedNumberRegisteredGroups = 0;
        final List<String> destinationsToRemove4 = Arrays.asList("DESTINATION", "DESTINATION1");
        client.addDestinations("LOGICAL-GROUP", destinationsToRemove4);
        client.removeDestinations("LOGICAL-GROUP", destinationsToRemove4);
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());

        // Test removal of multiple destinations.
        final Set<String> expectedDestinations = new HashSet<>(Arrays.asList("DESTINATION", "DESTINATION2"));
        final String currentTableEntryKey = "LOGICAL-GROUP";
        final List<String> destinationsToAdd = Arrays.asList("DESTINATION", "DESTINATION1", "DESTINATION2", "DESTINATION3");
        final List<String> destinationsToRemove5 = Arrays.asList("DESTINATION1", "DESTINATION3", "DESTINATION4", "DESTINATION5");
        client.addDestinations("LOGICAL-GROUP", destinationsToAdd);
        // Some destinations in destinationsToRemove5 do not exist, a waning is logged here but an
        // exception should not be thrown.
        client.removeDestinations("LOGICAL-GROUP", destinationsToRemove5);
        // No destinations in destinationsToRemove5 should exist after removal, a warning is logged
        // here but an exception should not be thrown.
        client.removeDestinations("LOGICAL-GROUP", destinationsToRemove5);
        Assert.assertEquals(expectedDestinations, new HashSet<>(sourceMetadataTable.entryStream()
                        .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                        .findFirst().get().getPayload().getDestinationIdsList()));

        // Test removal from a group that does not exist, warning is logged here.
        final Set<String> expectedDestinations1 = new HashSet<>(Arrays.asList("DESTINATION", "DESTINATION2"));
        final String currentTableEntryKey1 = "LOGICAL-GROUP";
        client.removeDestinations("LOGICAL-GROUP1", Collections.singletonList("DESTINATION"));
        Assert.assertEquals(expectedDestinations1, new HashSet<>(sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey1))
                .findFirst().get().getPayload().getDestinationIdsList()));
    }

    /**
     * Test getting destinations for a logical group.
     *
     */
    @Test
    public void testGetDestinations() {
        // Test get destinations with null/empty group.
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.getDestinations(null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.getDestinations(""));

        // Add destinations to get later.
        final List<String> destinationsToSet = Arrays.asList("DESTINATION", "DESTINATION1");
        client.setDestinations("LOGICAL-GROUP", destinationsToSet);

        // Test getting destinations.
        final Set<String> expectedDestinations = new HashSet<>(Arrays.asList("DESTINATION", "DESTINATION1"));
        Assert.assertEquals(expectedDestinations, new HashSet<>(client.getDestinations("LOGICAL-GROUP")));

        // Test get destinations on an invalid group.
        Assert.assertNull(client.getDestinations("LOGICAL-GROUP1"));
    }

    /**
     * Test setting destinations for a logical group.
     *
     */
    @Test
    public void testSetDestinations() {
        // Test setting with request having a malformed logicalGroup.
        final List<String> destinationsToSet = Collections.singletonList("DESTINATION");
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations("", destinationsToSet));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations(null, destinationsToSet));

        // Test setting  with request having a malformed destination.
        final List<String> destinationsToSet1 = Collections.singletonList("");
        final List<String> destinationsToSet2 = Collections.singletonList(null);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations("LOGICAL-GROUP", destinationsToSet1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations("LOGICAL-GROUP", destinationsToSet2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations("LOGICAL-GROUP", null));

        // Test clearing existing destination with passing empty list.
        final int expectedNumberRegisteredGroups = 0;
        final List<String> destinationsToSet3 = Arrays.asList("DESTINATION", "DESTINATION1");
        client.setDestinations("LOGICAL-GROUP", destinationsToSet3);
        // Check that destinations for the logical group is not empty.
        Assert.assertEquals(new HashSet<>(destinationsToSet3),
                new HashSet<>(client.getDestinations("LOGICAL-GROUP")));
        client.setDestinations("LOGICAL-GROUP", new ArrayList<>());
        // Check that the logical group was deleted after being emptied.
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());

        // Test setting list of destinations that contain a duplicate, list gets de-duplicated
        // and a warning is logged here.
        final int expectedNumberDestinations = 1;
        final String currentTableEntryKey = "LOGICAL-GROUP";
        final List<String> destinationsToSet4 = Arrays.asList("DESTINATION", "DESTINATION");
        client.setDestinations("LOGICAL-GROUP", destinationsToSet4);
        Assert.assertEquals(expectedNumberDestinations, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(currentTableEntryKey))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test init and consequent overwrite of a new logical group using setDestinations.
        final List<String> destinationsToSet5 = Arrays.asList("DESTINATION", "DESTINATION1");
        final List<String> destinationsToSet6 = Arrays.asList("DESTINATION", "DESTINATION1");
        client.setDestinations("LOGICAL-GROUP1", destinationsToSet5);
        Assert.assertEquals(new HashSet<>(destinationsToSet5),
                new HashSet<>(client.getDestinations("LOGICAL-GROUP1")));
        client.setDestinations("LOGICAL-GROUP1", destinationsToSet6);
        Assert.assertEquals(new HashSet<>(destinationsToSet6),
                new HashSet<>(client.getDestinations("LOGICAL-GROUP1")));

        // Test that the overall table size is correct.
        final int expectedNumberRegisteredGroups1 = 2;
        Assert.assertEquals(expectedNumberRegisteredGroups1, sourceMetadataTable.count());
    }

    /**
     * Test sample usage of client APIs.
     *
     */
    @Test
    public void testClientOperations() {
        final String logicalGroup = "LOGICAL-GROUP";
        final List<String> destination = Collections.singletonList("DESTINATION");
        final List<String> destinations = Arrays.asList("DESTINATION", "DESTINATION1");

        // Test addDestination does not add same destination twice.
        client.addDestinations(logicalGroup, destination);
        client.addDestinations(logicalGroup, destinations);
        Assert.assertEquals(new HashSet<>(destinations),
                new HashSet<>(client.getDestinations(logicalGroup)));

        final List<String> destinations1 = Arrays.asList("DESTINATION1", "DESTINATION2");
        // Test overwrite with setDestinations.
        client.setDestinations(logicalGroup, destinations1);
        Assert.assertEquals(new HashSet<>(destinations1),
                new HashSet<>(client.getDestinations(logicalGroup)));

        // Test removal of single destination, and consequent removal or remaining.
        final List<String> destination1 = Collections.singletonList("DESTINATION1");
        final int expectedNumberRegisteredGroups = 0;
        final List<String> destinationsAfterRemoval = Collections.singletonList("DESTINATION2");
        client.removeDestinations(logicalGroup, destination1);
        Assert.assertEquals(new HashSet<>(destinationsAfterRemoval),
                new HashSet<>(client.getDestinations(logicalGroup)));
        client.removeDestinations(logicalGroup, destinations1);
        // Number of groups should be 0 after logical group is emptied and deleted.
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());
    }
}
