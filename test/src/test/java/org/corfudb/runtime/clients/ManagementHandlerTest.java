package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;

import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.ManagementServer.ManagementServerInitializer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.junit.After;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the Management client.
 * <p>
 * Created by zlokhandwala on 11/7/16.
 */
public class ManagementHandlerTest extends AbstractClientTest {

    private ManagementClient client;
    private ManagementServer server;

    @Override
    Set<AbstractServer> getServersForTest() {
        ServerContext serverContext = new ServerContextBuilder()
                .setMemory(true)
                .setSingle(true)
                .setServerRouter(serverRouter)
                .setPort(SERVERS.PORT_0)
                .build();
        server = new ManagementServer(serverContext, new ManagementServerInitializer());
        serverRouter.setServerContext(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                // Required for management server to fetch the latest layout and connect runtime.
                .add(new LayoutServer(serverContext))
                // Required for management server to be able to bootstrap the sequencer.
                .add(new SequencerServer(serverContext))
                .add(new LogUnitServer(serverContext))
                .add(new BaseServer(serverContext))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        ManagementHandler managementHandler = new ManagementHandler();
        client = new ManagementClient(router, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"));
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseHandler())
                .add(managementHandler)
                .build();
    }

    /**
     * Need to shutdown the servers after test.
     */
    @After
    public void cleanUp() {
        server.shutdown();
    }

    /**
     * Tests the bootstrapping of the management server.
     *
     * @throws Exception error
     */
    @Test
    public void handleBootstrap()
            throws Exception {
        // Since the servers are started as single nodes thus already bootstrapped.
        assertThatThrownBy(() ->
                client.bootstrapManagement(TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    public void queryWorkflowRPCTest() throws Exception {
        // verify that non-active workflows return false when queried.
        QueryResponse resp = client.queryRequest(UUID.randomUUID());
        assertThat(resp.isActive()).isFalse();
    }

    /**
     * Tests the Node State request and asserts if response is received.
     *
     * @throws Exception error
     */
    @Test
    public void sendNodeStateRequest() throws Exception {
        NodeState nodeState= client.sendNodeStateRequest().get();
        assertThat(nodeState).isNotNull();
    }
}
