package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.junit.Test;

/**
 * Tests sealing scenarios.
 *
 * Created by zlokhandwala on 3/7/17.
 */
public class LayoutSealTest extends AbstractViewTest {

    /**
     * Gets a layout with 5 Servers:
     * PORT_0, PORT_1, PORT_2, PORT_3, PORT_4
     * Sets the replication view as specified.
     *
     * @param replicationMode Replication view to set all segments in the layout with.
     * @return  Built layout with a connected runtime.
     */
    public RuntimeLayout getRuntimeLayout(Layout.ReplicationMode replicationMode) {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        addServer(SERVERS.PORT_3);
        addServer(SERVERS.PORT_4);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setReplicationMode(replicationMode)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_3)
                .addLogUnit(SERVERS.PORT_4)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout(l);
        setAggressiveTimeouts(runtimeLayout);

        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();
        getManagementServer(SERVERS.PORT_3).shutdown();
        getManagementServer(SERVERS.PORT_4).shutdown();
        return runtimeLayout;
    }

    /**
     * Sets aggressive timeouts for all test routers.
     */
    public void setAggressiveTimeouts(RuntimeLayout runtimeLayout) {
        // Setting aggressive timeouts
        List<Integer> serverPorts = new ArrayList<>();
        serverPorts.add(SERVERS.PORT_0);
        serverPorts.add(SERVERS.PORT_1);
        serverPorts.add(SERVERS.PORT_2);
        serverPorts.add(SERVERS.PORT_3);
        serverPorts.add(SERVERS.PORT_4);
        List<String> routerEndpoints = new ArrayList<>();
        routerEndpoints.add(SERVERS.ENDPOINT_0);
        routerEndpoints.add(SERVERS.ENDPOINT_1);
        routerEndpoints.add(SERVERS.ENDPOINT_2);
        routerEndpoints.add(SERVERS.ENDPOINT_3);
        routerEndpoints.add(SERVERS.ENDPOINT_4);
        serverPorts.forEach(serverPort -> {
            routerEndpoints.forEach(routerEndpoint -> {
                runtimeLayout.getRuntime().getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtimeLayout.getRuntime().getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtimeLayout.getRuntime().getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            });
        });
    }

    public Map<Long, Integer> collectServerEpochs(int ... ports) {
        Map<Long, Integer> res = new HashMap<>();
        for (int x = 0; x < ports.length; x++) {
            long epoch = getLayoutServer(ports[x]).getServerContext().getServerEpoch();
            int count = res.getOrDefault(epoch, 0);
            res.put(epoch, count + 1);
        }
        return res;
    }

    /**
     * Asserts the Layout Servers Epochs.
     * Params: Expected epoch values
     */
    public void assertLayoutEpochs(long epochLayoutServer0, long epochLayoutServer1, long epochLayoutServer2) {
        assertThat(getLayoutServer(SERVERS.PORT_0).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer0);
        assertThat(getLayoutServer(SERVERS.PORT_1).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer1);
        assertThat(getLayoutServer(SERVERS.PORT_2).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer2);
    }

    /**
     * Asserts the Server Router's epochs
     * Params: Expected epoch values
     */
    public void assertServerRouterEpochs(long epochServerRouter0, long epochServerRouter1, long epochServerRouter2, long epochServerRouter3, long epochServerRouter4) {
        assertThat(getLayoutServer(SERVERS.PORT_0).getServerContext()
                .getServerRouter().getServerEpoch()).isEqualTo(epochServerRouter0);
        assertThat(getLayoutServer(SERVERS.PORT_1).getServerContext().getServerRouter()
                .getServerEpoch()).isEqualTo(epochServerRouter1);
        assertThat(getLayoutServer(SERVERS.PORT_2).getServerContext().getServerRouter()
                .getServerEpoch()).isEqualTo(epochServerRouter2);
        assertThat(getLayoutServer(SERVERS.PORT_3).getServerContext().getServerRouter()
                .getServerEpoch()).isEqualTo(epochServerRouter3);
        assertThat(getLayoutServer(SERVERS.PORT_4).getServerContext().getServerRouter()
                .getServerEpoch()).isEqualTo(epochServerRouter4);
    }

    /**
     * Scenario: 5 Servers.
     * All working normally and attempted to seal.
     * Seal passes.
     */
    @Test
    public void successfulChainSeal() {
        RuntimeLayout runtimeLayout = getRuntimeLayout(Layout.ReplicationMode.CHAIN_REPLICATION);
        Layout l = runtimeLayout.getLayout();
        l.setEpoch(l.getEpoch() + 1);
        runtimeLayout.sealMinServerSet();

        int[] ports = l.getAllServers().stream().mapToInt(s -> Integer.parseInt(s.split(":")[1])).toArray();
        Map<Long, Integer> epochs = collectServerEpochs(ports);
        // Check that we have a quorum of "sealed epochs"
        assertThat(epochs.get(l.getEpoch())).isGreaterThanOrEqualTo((ports.length / 2) + 1);
    }

    /**
     * Scenario: 5 Servers.
     * ENDPOINT_1, ENDPOINT_3 and ENDPOINT_4 failed and attempted to seal.
     * LayoutServers quorum is possible,    -   Seal passes
     * Stripe 1: 1 failed, 2 responses.     -   Seal passes
     * Stripe 2: 2 failed, 0 responses.     -   Seal failed
     * Seal failed
     */
    @Test
    public void failingChainSeal() {
        RuntimeLayout runtimeLayout = getRuntimeLayout(Layout.ReplicationMode.CHAIN_REPLICATION);
        Layout l = runtimeLayout.getLayout();

        addClientRule(runtimeLayout.getRuntime(), SERVERS.ENDPOINT_1, new TestRule().drop().always());
        addClientRule(runtimeLayout.getRuntime(), SERVERS.ENDPOINT_3, new TestRule().drop().always());
        addClientRule(runtimeLayout.getRuntime(), SERVERS.ENDPOINT_4, new TestRule().drop().always());

        l.setEpoch(l.getEpoch() + 1);
        assertThatThrownBy(runtimeLayout::sealMinServerSet)
                .isInstanceOf(QuorumUnreachableException.class);
        assertLayoutEpochs(2, 1, 2);
        assertServerRouterEpochs(2, 1, 2, 1, 1);
    }
}
