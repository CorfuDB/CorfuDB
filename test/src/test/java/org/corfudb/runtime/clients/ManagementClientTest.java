package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the Management client.
 * <p>
 * Created by zlokhandwala on 11/7/16.
 */
public class ManagementClientTest extends AbstractClientTest {

    ManagementClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        ServerContext serverContext = defaultServerContext();
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new ManagementServer(serverContext))
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
     * Tests the bootstrapping of the management server.
     * @throws Exception
     */
    @Test
    public void handleBootstrap()
            throws Exception {

        assertThat(client.bootstrapManagement(TestLayoutBuilder.single(9000)).get()).isEqualTo(true);
    }

    /**
     * Tests the msg handler for failure detection.
     * @throws Exception
     */
    @Test
    public void handleFailure()
            throws Exception {

        client.bootstrapManagement(TestLayoutBuilder.single(9000)).get();
        Map map = new HashMap<String, Boolean>();
        map.put("Key", true);
        assertThat(client.handleFailure(map).get()).isEqualTo(true);
    }
}
