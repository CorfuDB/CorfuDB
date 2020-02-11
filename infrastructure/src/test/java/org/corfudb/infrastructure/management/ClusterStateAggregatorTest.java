package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

public class ClusterStateAggregatorTest {

    private final long epoch = 1;

    @Test
    public void getAggregatedStateSingleNodeCluster() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK)
        );

        List<ClusterState> clusterStates = Arrays.asList(clusterState, clusterState, clusterState);
        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        NodeState expectedNodeState = clusterState.getNode("a").get();
        NodeState actualNodeState = aggregator.getAggregatedState().getNode("a").get();
        assertThat(actualNodeState).isEqualTo(expectedNodeState);
    }

    @Test
    public void getAggregatedState() {
        final String localEndpoint = "a";

        ClusterState clusterState1 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getUnavailableNodeState("c")
        );

        ClusterState clusterState2 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, FAILED),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        final int epoch = 1;
        final int counter = 123;
        ClusterState clusterState3 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getNotReadyNodeState("c")
        );

        List<ClusterState> clusterStates = Arrays.asList(
                clusterState1, clusterState2, clusterState3
        );

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        //check [CONNECTED, CONNECTED, CONNECTED]
        NodeState expectedLocalNodeState = clusterState3.getNode(localEndpoint).get();
        NodeState localNodeState = aggregator.getAggregatedState().getNode(localEndpoint).get();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeState).isEqualTo(expectedLocalNodeState);

        //check [UNAVAILABLE, CONNECTED, UNAVAILABLE]
        NodeState expectedNodeBState = clusterState2.getNode("b").get();
        NodeState nodeBState = aggregator.getAggregatedState().getNode("b").get();
        assertThat(nodeBState.isConnected()).isTrue();
        assertThat(nodeBState).isEqualTo(expectedNodeBState);

        //check [UNAVAILABLE, UNAVAILABLE, NOT_READY]
        NodeState expectedNodeCState = clusterState3.getNode("c").get();
        NodeState nodeCState = aggregator.getAggregatedState().getNode("c").get();
        assertThat(nodeCState.isConnected()).isFalse();
        assertThat(nodeCState).isEqualTo(expectedNodeCState);
    }
}
