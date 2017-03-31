package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
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
     * PORT_0, PORT_1, PORT_2, PORT_3, PORT_4
     * Sets the replication view as specified.
     *
     * @param replicationMode Replication view to set all segments in the layout with.
     * @return  Built layout with a connected runtime.
     */
    public Layout getLayout(Layout.ReplicationMode replicationMode) {
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
        l.setRuntime(corfuRuntime);
        setAggressiveTimeouts(l);
        return l;
    }

    /**
     * Sets aggressive timeouts for all test routers.
     */
    public void setAggressiveTimeouts(Layout layout) {
        // Setting aggressive timeouts
        List<Integer> serverPorts = new ArrayList<>();
        serverPorts.add(SERVERS.PORT_0);
        serverPorts.add(SERVERS.PORT_1);
        serverPorts.add(SERVERS.PORT_2);
        serverPorts.add(SERVERS.PORT_3);
        serverPorts.add(SERVERS.PORT_4);
        List<String> routerEndpoints = new ArrayList<> ();
        routerEndpoints.add(SERVERS.ENDPOINT_0);
        routerEndpoints.add(SERVERS.ENDPOINT_1);
        routerEndpoints.add(SERVERS.ENDPOINT_2);
        routerEndpoints.add(SERVERS.ENDPOINT_3);
        routerEndpoints.add(SERVERS.ENDPOINT_4);
        serverPorts.forEach(serverPort -> {
            routerEndpoints.forEach(routerEndpoint -> {
                layout.getRuntime().getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                layout.getRuntime().getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                layout.getRuntime().getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            });
        });
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
        assertThat(getLayoutServer(SERVERS.PORT_0).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter0);
        assertThat(getLayoutServer(SERVERS.PORT_1).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter1);
        assertThat(getLayoutServer(SERVERS.PORT_2).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter2);
        assertThat(getLayoutServer(SERVERS.PORT_3).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter3);
        assertThat(getLayoutServer(SERVERS.PORT_4).getServerContext().getServerEpoch()).isEqualTo(epochServerRouter4);
    }

    /**
     * Scenario: 5 Servers.
     * All working normally and attempted to seal.
     * Seal passes.
     */
    @Test
    public void successfulChainSeal() {
        Layout l = getLayout(Layout.ReplicationMode.CHAIN_REPLICATION);
        l.setEpoch(l.getEpoch() + 1);
        try {
            l.moveServersToEpoch();
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
        Layout l = getLayout(Layout.ReplicationMode.CHAIN_REPLICATION);

        addClientRule(l.getRuntime(), SERVERS.ENDPOINT_1, new TestRule().drop().always());
        addClientRule(l.getRuntime(), SERVERS.ENDPOINT_3, new TestRule().drop().always());
        addClientRule(l.getRuntime(), SERVERS.ENDPOINT_4, new TestRule().drop().always());

        l.setEpoch(l.getEpoch() + 1);
        assertThatThrownBy(() -> l.moveServersToEpoch()).isInstanceOf(QuorumUnreachableException.class);
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
        Layout l = getLayout(Layout.ReplicationMode.QUORUM_REPLICATION);
        l.setEpoch(l.getEpoch() + 1);
        try {
            l.moveServersToEpoch();
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
        Layout l = getLayout(Layout.ReplicationMode.QUORUM_REPLICATION);

        addClientRule(l.getRuntime(), SERVERS.ENDPOINT_3, new TestRule().drop().always());

        l.setEpoch(l.getEpoch() + 1);
        assertThatThrownBy(() -> l.moveServersToEpoch()).isInstanceOf(QuorumUnreachableException.class);
        assertLayoutEpochs(2, 2, 2);
        assertServerRouterEpochs(2, 2, 2, 1, 2);
    }
}
