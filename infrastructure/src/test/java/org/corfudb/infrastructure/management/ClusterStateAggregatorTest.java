package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.common.result.Result;
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

    @Test
    public void getAggregatedStateSingleNodeCluster() {
        final String localEndpoint = "a";
        final long epoch = 1;

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK)
        );

        List<Result<ClusterState, IllegalStateException>> clusterStates = Arrays.asList(
                Result.ok(clusterState), Result.ok(clusterState), Result.ok(clusterState)
        );
        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        NodeState expectedNodeState = clusterState.getNode("a").get();
        NodeState actualNodeState = aggregator.getAggregatedState().get().getNode("a").get();
        assertThat(actualNodeState).isEqualTo(expectedNodeState);
    }

    @Test
    public void getInvalidAggregatedState() {
        final String localEndpoint = "a";
        final long epoch1 = 1;
        final long epoch2 = 2;

        ClusterState clusterState1 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch1, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getUnavailableNodeState("c")
        );

        ClusterState clusterState2 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch1, OK, OK, FAILED),
                nodeState("b", epoch1, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        ClusterState clusterState3 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch2, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getNotReadyNodeState("c")
        );

        List<Result<ClusterState, IllegalStateException>> clusterStates = Arrays.asList(
                Result.ok(clusterState1), Result.ok(clusterState2), Result.ok(clusterState3)
        );

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        aggregator.getAggregatedState();
    }

    @Test
    public void getAggregatedState() {
        final String localEndpoint = "a";
        final long epoch = 1;

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

        ClusterState clusterState3 = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getNotReadyNodeState("c")
        );

        List<Result<ClusterState, IllegalStateException>> clusterStates = Arrays.asList(
                Result.ok(clusterState1), Result.ok(clusterState2), Result.ok(clusterState3)
        );

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        //check [CONNECTED, CONNECTED, CONNECTED]
        NodeState expectedLocalNodeState = clusterState3.getNode(localEndpoint).get();
        NodeState localNodeState = aggregator.getAggregatedState().get().getNode(localEndpoint).get();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeState).isEqualTo(expectedLocalNodeState);

        //check [UNAVAILABLE, CONNECTED, UNAVAILABLE]
        NodeState expectedNodeBState = clusterState2.getNode("b").get();
        NodeState nodeBState = aggregator.getAggregatedState().get().getNode("b").get();
        assertThat(nodeBState.isConnected()).isTrue();
        assertThat(nodeBState).isEqualTo(expectedNodeBState);

        //check [UNAVAILABLE, UNAVAILABLE, NOT_READY]
        NodeState expectedNodeCState = clusterState3.getNode("c").get();
        NodeState nodeCState = aggregator.getAggregatedState().get().getNode("c").get();
        assertThat(nodeCState.isConnected()).isFalse();
        assertThat(nodeCState).isEqualTo(expectedNodeCState);
    }
}
