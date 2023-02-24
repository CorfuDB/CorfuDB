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

    // Sample variables used for testing.
    private static final String clientName = "client";
    private static final String client1 = "client1";
    private static final String client2 = "client2";
    private static final String logicalGroup = "LOGICAL-GROUP";
    private static final String logicalGroup1 = "LOGICAL-GROUP1";
    private static final String destination = "DESTINATION";
    private static final String destination1 = "DESTINATION1";
    private static final String destination2 = "DESTINATION2";
    private static final String destination3 = "DESTINATION3";
    private static final String destination4 = "DESTINATION4";
    private static final String destination5 = "DESTINATION5";

    private static Table<ClientRegistrationId, ClientRegistrationInfo, ManagedResources> replicationRegistrationTable;
    private static Table<ClientDestinationInfoKey, DestinationInfoVal, ManagedResources> sourceMetadataTable;
    private static CorfuRuntime runtime;
    private static LogReplicationLogicalGroupClient client;

    private CorfuRuntime getTestRuntime() {
        return getDefaultRuntime();
    }

    @Before
    public void loadProperties() throws Exception {
        runtime = getTestRuntime().connect();
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
        new LogReplicationLogicalGroupClient(runtime, client1);
        new LogReplicationLogicalGroupClient(runtime, client2);
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
        final List<String> destinationsToAdd = Collections.singletonList(destination);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations("", destinationsToAdd));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(null, destinationsToAdd));

        // Test adding multiple destinations with request having a malformed destination.
        final List<String> destinationsToAdd1 = Collections.singletonList("");
        final List<String> destinationsToAdd2 = Collections.singletonList(null);
        final List<String> destinationsToAdd3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(logicalGroup, destinationsToAdd1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(logicalGroup, destinationsToAdd2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(logicalGroup, destinationsToAdd3));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.addDestinations(logicalGroup, null));

        // Test adding list of destinations that contain a duplicate, list gets de-duplicated
        // and a warning is logged here.
        final int expectedNumberDestinations = 1;
        final List<String> destinationsToAdd4 = Arrays.asList(destination, destination);
        client.addDestinations(logicalGroup, destinationsToAdd4);
        Assert.assertEquals(expectedNumberDestinations, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(logicalGroup))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test adding destinations to a second logical group.
        final int expectedNumberDestinations2 = 4;
        final List<String> destinationsToAdd5 = Arrays.asList(destination, destination1);
        final List<String> destinationsToAdd6 = Arrays.asList(destination2, destination3);
        client.addDestinations(logicalGroup1, destinationsToAdd5);
        // Test adding destinations that already exist, warning is logged here.
        client.addDestinations(logicalGroup1, destinationsToAdd5);
        client.addDestinations(logicalGroup1, destinationsToAdd6);
        Assert.assertEquals(expectedNumberDestinations2, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(logicalGroup1))
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
        final List<String> destinationsToRemove = Collections.singletonList(destination);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations("", destinationsToRemove));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(null, destinationsToRemove));

        // Test removing multiple destinations with request having a malformed destination.
        final List<String> destinationsToRemove1 = Collections.singletonList("");
        final List<String> destinationsToRemove2 = Collections.singletonList(null);
        final List<String> destinationsToRemove3 = new ArrayList<>();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(logicalGroup, destinationsToRemove1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(logicalGroup, destinationsToRemove2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(logicalGroup, destinationsToRemove3));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.removeDestinations(logicalGroup, null));

        // Test add then remove, group should be deleted once empty.
        final int expectedNumberRegisteredGroups = 0;
        final List<String> destinationsToRemove4 = Arrays.asList(destination, destination1);
        client.addDestinations(logicalGroup, destinationsToRemove4);
        client.removeDestinations(logicalGroup, destinationsToRemove4);
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());

        // Test removal of multiple destinations.
        final Set<String> expectedDestinations = new HashSet<>(Arrays.asList(destination, destination2));
        final List<String> destinationsToAdd = Arrays.asList(destination, destination1, destination2, destination3);
        final List<String> destinationsToRemove5 = Arrays.asList(destination1, destination3, destination4, destination5);
        client.addDestinations(logicalGroup, destinationsToAdd);
        // Some destinations in destinationsToRemove5 do not exist, a waning is logged here but an
        // exception should not be thrown.
        client.removeDestinations(logicalGroup, destinationsToRemove5);
        // No destinations in destinationsToRemove5 should exist after removal, a warning is logged
        // here but an exception should not be thrown.
        client.removeDestinations(logicalGroup, destinationsToRemove5);
        Assert.assertEquals(expectedDestinations, new HashSet<>(sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(logicalGroup))
                .findFirst().get().getPayload().getDestinationIdsList()));

        // Test removal from a group that does not exist, warning is logged here.
        final Set<String> expectedDestinations1 = new HashSet<>(Arrays.asList(destination, destination2));
        client.removeDestinations(logicalGroup1, Collections.singletonList(destination));
        Assert.assertEquals(expectedDestinations1, new HashSet<>(sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(logicalGroup))
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
        final List<String> destinationsToSet = Arrays.asList(destination, destination1);
        client.setDestinations(logicalGroup, destinationsToSet);

        // Test getting destinations.
        final Set<String> expectedDestinations = new HashSet<>(Arrays.asList(destination, destination1));
        Assert.assertEquals(expectedDestinations, new HashSet<>(client.getDestinations(logicalGroup)));

        // Test get destinations on an invalid group.
        Assert.assertNull(client.getDestinations(logicalGroup1));
    }

    /**
     * Test setting destinations for a logical group.
     *
     */
    @Test
    public void testSetDestinations() {
        // Test setting with request having a malformed logicalGroup.
        final List<String> destinationsToSet = Collections.singletonList(destination);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations("", destinationsToSet));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations(null, destinationsToSet));

        // Test setting  with request having a malformed destination.
        final List<String> destinationsToSet1 = Collections.singletonList("");
        final List<String> destinationsToSet2 = Collections.singletonList(null);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations(logicalGroup, destinationsToSet1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations(logicalGroup, destinationsToSet2));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> client.setDestinations(logicalGroup, null));

        // Test clearing existing destination with passing empty list.
        final int expectedNumberRegisteredGroups = 0;
        final List<String> destinationsToSet3 = Arrays.asList(destination, destination1);
        client.setDestinations(logicalGroup, destinationsToSet3);
        // Check that destinations for the logical group is not empty.
        Assert.assertEquals(new HashSet<>(destinationsToSet3),
                new HashSet<>(client.getDestinations(logicalGroup)));
        client.setDestinations(logicalGroup, new ArrayList<>());
        // Check that the logical group was deleted after being emptied.
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());

        // Test setting list of destinations that contain a duplicate, list gets de-duplicated
        // and a warning is logged here.
        final int expectedNumberDestinations = 1;
        final List<String> destinationsToSet4 = Arrays.asList(destination, destination);
        client.setDestinations(logicalGroup, destinationsToSet4);
        Assert.assertEquals(expectedNumberDestinations, sourceMetadataTable.entryStream()
                .filter(e -> e.getKey().getGroupName().equals(logicalGroup))
                .findFirst().get().getPayload().getDestinationIdsList().size());

        // Test init and consequent overwrite of a new logical group using setDestinations.
        final List<String> destinationsToSet5 = Arrays.asList(destination, destination1);
        final List<String> destinationsToSet6 = Arrays.asList(destination, destination1);
        client.setDestinations(logicalGroup1, destinationsToSet5);
        Assert.assertEquals(new HashSet<>(destinationsToSet5),
                new HashSet<>(client.getDestinations(logicalGroup1)));
        client.setDestinations(logicalGroup1, destinationsToSet6);
        Assert.assertEquals(new HashSet<>(destinationsToSet6),
                new HashSet<>(client.getDestinations(logicalGroup1)));

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
        final List<String> destinationsToAdd = Collections.singletonList(destination);
        final List<String> destinationsToAdd1 = Arrays.asList(destination, destination1);
        // Test addDestination does not add same destination twice.
        client.addDestinations(logicalGroup, destinationsToAdd);
        client.addDestinations(logicalGroup, destinationsToAdd1);
        Assert.assertEquals(new HashSet<>(destinationsToAdd1),
                new HashSet<>(client.getDestinations(logicalGroup)));

        final List<String> destinationsToSet = Arrays.asList(destination1, destination2);
        // Test overwrite with setDestinations.
        client.setDestinations(logicalGroup, destinationsToSet);
        Assert.assertEquals(new HashSet<>(destinationsToSet),
                new HashSet<>(client.getDestinations(logicalGroup)));

        // Test removal of single destination, and consequent removal or remaining.
        final List<String> destinationsToRemove = Collections.singletonList(destination1);
        final List<String> destinationsToRemove2 = Arrays.asList(destination1, destination2);
        final int expectedNumberRegisteredGroups = 0;
        final List<String> expectedDestinations = Collections.singletonList(destination2);
        client.removeDestinations(logicalGroup, destinationsToRemove);
        Assert.assertEquals(new HashSet<>(expectedDestinations),
                new HashSet<>(client.getDestinations(logicalGroup)));
        client.removeDestinations(logicalGroup, destinationsToRemove2);
        // Number of groups should be 0 after logical group is emptied and deleted.
        Assert.assertEquals(expectedNumberRegisteredGroups, sourceMetadataTable.count());
    }
}
