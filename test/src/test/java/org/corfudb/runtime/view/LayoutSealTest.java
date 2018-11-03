package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests sealing scenarios.
 *
 * Created by zlokhandwala on 3/7/17.
 */
public class LayoutSealTest extends AbstractViewTest {

    /**
     * Gets a layout with 5 Servers:
     * ENDPOINT_0, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4
     * Sets the replication view as specified.
     *
     * @param replicationMode Replication view to set all segments in the layout with.
     * @return  Built layout with a connected runtime.
     */
    public RuntimeLayout getRuntimeLayout(Layout.ReplicationMode replicationMode) {
        addServer(SERVERS.ENDPOINT_0);
        addServer(SERVERS.ENDPOINT_1);
        addServer(SERVERS.ENDPOINT_2);
        addServer(SERVERS.ENDPOINT_3);
        addServer(SERVERS.ENDPOINT_4);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .buildSegment()
                .setReplicationMode(replicationMode)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addLogUnit(SERVERS.ENDPOINT_4)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout(l);
        setAggressiveTimeouts(runtimeLayout);

        getManagementServer(SERVERS.ENDPOINT_0).shutdown();
        getManagementServer(SERVERS.ENDPOINT_1).shutdown();
        getManagementServer(SERVERS.ENDPOINT_2).shutdown();
        getManagementServer(SERVERS.ENDPOINT_3).shutdown();
        getManagementServer(SERVERS.ENDPOINT_4).shutdown();
        return runtimeLayout;
    }

    /**
     * Sets aggressive timeouts for all test routers.
     */
    public void setAggressiveTimeouts(RuntimeLayout runtimeLayout) {
        // Setting aggressive timeouts
        List<NodeLocator> serverPorts = new ArrayList<>();
        serverPorts.add(SERVERS.ENDPOINT_0);
        serverPorts.add(SERVERS.ENDPOINT_1);
        serverPorts.add(SERVERS.ENDPOINT_2);
        serverPorts.add(SERVERS.ENDPOINT_3);
        serverPorts.add(SERVERS.ENDPOINT_4);

        List<NodeLocator> routerEndpoints = new ArrayList<>();
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

    /**
     * Asserts the Layout Servers Epochs.
     * Params: Expected epoch values
     */
    public void assertLayoutEpochs(long epochLayoutServer0, long epochLayoutServer1, long epochLayoutServer2) {
        assertThat(getLayoutServer(SERVERS.ENDPOINT_0).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer0);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_1).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer1);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_2).getServerContext().getServerEpoch()).isEqualTo(epochLayoutServer2);
    }

    /**
     * Asserts the Server Router's epochs
     * Params: Expected epoch values
     */
    public void assertServerRouterEpochs(long epochServerRouter0, long epochServerRouter1, long epochServerRouter2, long epochServerRouter3, long epochServerRouter4) {
        assertThat(getLayoutServer(SERVERS.ENDPOINT_0).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter0);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_1).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter1);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_2).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter2);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_3).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter3);
        assertThat(getLayoutServer(SERVERS.ENDPOINT_4).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter4);
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
        try {
            runtimeLayout.sealMinServerSet();
        } catch (QuorumUnreachableException e) {
            e.printStackTrace();
        }
        assertLayoutEpochs(2, 2, 2);
        assertServerRouterEpochs(2, 2, 2, 2, 2);
    }

    /**
     * Scenario: 5 Servers.
     * ENDPOINT_1, ENDPOINT_3 and ENDPOINT_3 failed and attempted to seal.
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

    /**
     * Scenario: 5 Servers.
     * All working normally and attempted to seal.
     * Seal passes.
     */
    @Test
    public void successfulQuorumSeal() {
        RuntimeLayout runtimeLayout = getRuntimeLayout(Layout.ReplicationMode.QUORUM_REPLICATION);
        Layout l = runtimeLayout.getLayout();
        l.setEpoch(l.getEpoch() + 1);
        try {
            runtimeLayout.sealMinServerSet();
        } catch (QuorumUnreachableException e) {
            e.printStackTrace();
        }
        assertLayoutEpochs(2, 2, 2);
        assertServerRouterEpochs(2, 2, 2, 2, 2);
    }

    /**
     * Scenario: 5 Servers.
     * ENDPOINT_3 failed and attempted to seal.
     * LayoutServers quorum is possible,    -   Seal passes
     * Stripe 1: 0 failed, 3 responses.     -   Seal passes
     * Stripe 2: 1 failed, 1 response.      -   Seal failed (Quorum not possible)
     * Seal failed
     */
    @Test
    public void failingQuorumSeal() {
        RuntimeLayout runtimeLayout = getRuntimeLayout(Layout.ReplicationMode.QUORUM_REPLICATION);
        Layout l = runtimeLayout.getLayout();

        addClientRule(runtimeLayout.getRuntime(), SERVERS.ENDPOINT_3,
                new TestRule().drop().always());

        l.setEpoch(l.getEpoch() + 1);
        assertThatThrownBy(runtimeLayout::sealMinServerSet)
                .isInstanceOf(QuorumUnreachableException.class);
        assertLayoutEpochs(2, 2, 2);
        assertServerRouterEpochs(2, 2, 2, 1, 2);
    }
}
