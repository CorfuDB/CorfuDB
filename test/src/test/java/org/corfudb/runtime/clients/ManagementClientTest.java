package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.*;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
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
        final int MAX_CACHE = 256_000_000;
        ServerContext serverContext = new ServerContextBuilder()
                .setInitialToken(0)
                .setMemory(true)
                .setSingle(true)
                .setMaxCache(MAX_CACHE)
                .setServerRouter(serverRouter)
                .build();
        server = new ManagementServer(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                .add(new LayoutServer(serverContext))
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
        assertThatThrownBy(() -> client.bootstrapManagement(TestLayoutBuilder.single(SERVERS.PORT_0)).get()).isInstanceOf(ExecutionException.class);
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
        Map map = new HashMap<String, Boolean>();
        map.put("Key", true);
        assertThat(client.handleFailure(map).get()).isEqualTo(true);
    }
}
