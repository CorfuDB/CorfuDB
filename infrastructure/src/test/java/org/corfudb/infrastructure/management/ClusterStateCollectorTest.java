package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

public class ClusterStateCollectorTest {
    private final long epoch = 1;

    /**
     * Checks the aggregated cluster state.
     * Cluster state is:
     * - node A is a local node
     * - node A connected to node B,
     * - node C - replied with wrong epoch exception
     * The test contains following scenarios:
     * - node A: check that node A is connected to all other nodes
     * - node B: connected to A
     * - node C: the node state is {@link NodeConnectivityType.UNAVAILABLE}
     */
    @Test
    public void testAggregatedState() {
        final String localEndpoint = "a";

        ClusterState predefinedCluster = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, FAILED),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        Map<String, CompletableFuture<NodeState>> clusterConnectivity = new HashMap<>();

        CompletableFuture<NodeState> nodeAStateCf = CompletableFuture.completedFuture(
                predefinedCluster.getNode("a").get()
        );

        CompletableFuture<NodeState> nodeBStateCf = CompletableFuture.completedFuture(
                predefinedCluster.getNode("b").get()
        );

        final int newEpoch = 10;
        CompletableFuture<NodeState> wrongEpochCf = new CompletableFuture<>();
        wrongEpochCf.completeExceptionally(new WrongEpochException(newEpoch));

        clusterConnectivity.put("a", nodeAStateCf);
        clusterConnectivity.put("b", nodeBStateCf);
        clusterConnectivity.put("c", wrongEpochCf);

        ClusterStateCollector collector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(clusterConnectivity)
                .build();

        ClusterState clusterState = collector.collectClusterState(
                1, ImmutableList.of(), SequencerMetrics.UNKNOWN
        );

        NodeState localNodeState = clusterState.getNode(localEndpoint).get();
        NodeConnectivity localNodeConnectivity = localNodeState.getConnectivity();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeConnectivity.getConnectedNodes()).containsExactly("a", "b", "c");
        assertThat(localNodeConnectivity.getFailedNodes()).isEmpty();

        NodeState nodeBState = clusterState.getNode("b").get();
        NodeConnectivity nodeBConnectivity = nodeBState.getConnectivity();
        assertThat(nodeBConnectivity.getConnectedNodes()).containsExactly("a", "b");

        NodeState nodeCState = clusterState.getNode("c").get();
        assertThat(nodeCState.isConnected()).isFalse();
    }

    /**
     * Check that the collector can collect wrong epochs from the cluster info classes.
     */
    @Test
    public void testWrongEpochs() {
        final String localEndpoint = "a";

        Map<String, CompletableFuture<NodeState>> clusterConnectivity = new HashMap<>();

        final int newEpoch = 10;
        CompletableFuture<NodeState> wrongEpochCf = new CompletableFuture<>();
        wrongEpochCf.completeExceptionally(new WrongEpochException(newEpoch));

        CompletableFuture<NodeState> nodeAState = CompletableFuture.completedFuture(
                NodeState.builder().build()
        );

        clusterConnectivity.put("a", nodeAState);
        clusterConnectivity.put("c", wrongEpochCf);

        ClusterStateCollector collector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(clusterConnectivity)
                .build();

        ImmutableMap<String, Long> wrongEpochs = collector.collectWrongEpochs();
        assertThat(wrongEpochs.size()).isOne();
        assertThat(wrongEpochs.get("c")).isEqualTo(newEpoch);
    }

}
