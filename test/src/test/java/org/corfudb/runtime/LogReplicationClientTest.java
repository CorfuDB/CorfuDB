package org.corfudb.runtime;

import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.LogReplication.ClientInfo;
import org.corfudb.runtime.LogReplication.ClientInfoKey;
import org.corfudb.runtime.LogReplication.SinksInfoVal;

import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.collections.CorfuStore;
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

public class LogReplicationClientTest extends AbstractIT {
    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
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
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * Test registering replication client
     *
     * @throws Exception
     */
    @Test
    public void testRegisterReplicationClient() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationRegistrationTable";
        final String client_name = "client";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<StringKey, ClientInfo, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                StringKey.class,
                ClientInfo.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, client_name);

        Boolean canAddNewClient = client.registerReplicationClient();
        Boolean tryAddingExistingClient = client.registerReplicationClient();

        Assert.assertEquals(canAddNewClient, true);
        Assert.assertEquals(tryAddingExistingClient, false);
    }

    /**
     * Test add destination
     *
     * @throws Exception
     */
    @Test
    public void testAddDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String client_name = "client";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, client_name);

        Boolean canAdd1 = client.addDestination("DOMAIN", "DESTINATION1");
        Boolean canAdd2 = client.addDestination("DOMAIN", "DESTINATION2");
        Boolean canAddExisting = client.addDestination("DOMAIN", "DESTINATION1");

        Assert.assertEquals(canAdd1, true);
        Assert.assertEquals(canAdd2, true);
        Assert.assertEquals(canAddExisting, false);
    }

    /**
     * Test add multiple destinations
     *
     * @throws Exception
     */
    @Test
    public void testAddListOfDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String client_name = "client";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, client_name);

        List<String> destinationsToAdd = Arrays.asList("DESTINATION1", "DESTINATION1", "DESTINATION2", "DESTINATION3");
        Boolean canAdd = client.addDestination("DOMAIN", destinationsToAdd);

        List<String> repeatDestinationsToAdd = Collections.singletonList("DESTINATION1");
        Boolean canAddRepeatDestination = client.addDestination("DOMAIN", repeatDestinationsToAdd);

        Assert.assertEquals(canAdd, true);
        Assert.assertEquals(canAddRepeatDestination, false);
    }

    /**
     * Test remove destination
     *
     * @throws Exception
     */
    @Test
    public void removeDestination() throws Exception {
        final String namespace = "CorfuSystem";
        final String tableName = "LogReplicationSourceMetadataTable";
        final String client_name = "client";

        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<ClientInfoKey, SinksInfoVal, SampleSchema.ManagedResources> table = store.openTable(
                namespace,
                tableName,
                ClientInfoKey.class,
                SinksInfoVal.class,
                SampleSchema.ManagedResources.class,
                TableOptions.fromProtoSchema(SampleSchema.Uuid.class));

        LogReplicationClient client = new LogReplicationClient(runtime, client_name);

        Boolean canRemoveNonexistent = client.removeDestination("DOMAIN", "DESTINATION");
        Boolean canAdd = client.addDestination("DOMAIN", "DESTINATION");
        Boolean canRemove = client.removeDestination("DOMAIN", "DESTINATION");

        Assert.assertEquals(canRemoveNonexistent, false);
        Assert.assertEquals(canAdd, true);
        Assert.assertEquals(canRemove, true);
    }
}
