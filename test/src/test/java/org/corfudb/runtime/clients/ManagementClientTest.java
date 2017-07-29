package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.format.Types.NodeMetrics;
import org.corfudb.infrastructure.*;
import org.junit.After;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the Management client.
 * <p>
 * Created by zlokhandwala on 11/7/16.
 */
public class ManagementClientTest extends AbstractClientTest {

    ManagementClient client;
    ManagementServer server;

    @Override
    Set<AbstractServer> getServersForTest() {
        ServerContext serverContext = new ServerContextBuilder()
                .setInitialToken(0)
                .setMemory(true)
                .setSingle(true)
                .setServerRouter(serverRouter)
                .build();
        server = new ManagementServer(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                // Required for management server to fetch the latest layout and connect runtime.
                .add(new LayoutServer(serverContext))
                // Required for management server to be able to bootstrap the sequencer.
                .add(new SequencerServer(serverContext))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new ManagementClient();
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseClient())
                .add(client)
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
     * @throws Exception
     */
    @Test
    public void handleBootstrap()
            throws Exception {
        // Since the servers are started as single nodes thus already bootstrapped.
        assertThatThrownBy(() ->
                client.bootstrapManagement(TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .isInstanceOf(ExecutionException.class);
    }

    /**
     * Tests the msg handler for failure detection.
     *
     * @throws Exception
     */
    @Test
    public void handleFailure()
            throws Exception {

        // Since the servers are started as single nodes thus already bootstrapped.
        Set<String> set = new HashSet<>();
        set.add("Key");
        assertThat(client.handleFailure(set).get()).isEqualTo(true);
    }

    /**
     * Tests the failure handler start trigger.
     *
     * @throws Exception
     */
    @Test
    public void initiateFailureHandler()
            throws Exception {
        assertThat(client.initiateFailureHandler().get()).isEqualTo(true);
    }

    /**
     * Tests the heartbeat request and asserts if response is received.
     *
     * @throws Exception
     */
    @Test
    public void sendHeartbeatRequest()
            throws Exception {
        byte[] buffer = client.sendHeartbeatRequest().get();
        assertThat(NodeMetrics.parseFrom(buffer)).isNotNull();
    }
}
