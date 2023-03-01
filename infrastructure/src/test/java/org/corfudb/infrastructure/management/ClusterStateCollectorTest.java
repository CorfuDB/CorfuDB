package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.B;
import static org.corfudb.infrastructure.NodeNames.C;
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
        final String localEndpoint = A;

        ClusterState predefinedCluster = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, OK, FAILED),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );

        Map<String, CompletableFuture<NodeState>> clusterConnectivity = new HashMap<>();

        CompletableFuture<NodeState> nodeAStateCf = CompletableFuture.completedFuture(
                predefinedCluster.getNode(localEndpoint).get()
        );

        CompletableFuture<NodeState> nodeBStateCf = CompletableFuture.completedFuture(
                predefinedCluster.getNode(B).get()
        );

        final int newEpoch = 10;
        CompletableFuture<NodeState> wrongEpochCf = new CompletableFuture<>();
        wrongEpochCf.completeExceptionally(new WrongEpochException(newEpoch));

        clusterConnectivity.put(localEndpoint, nodeAStateCf);
        clusterConnectivity.put(B, nodeBStateCf);
        clusterConnectivity.put(C, wrongEpochCf);

        ClusterStateCollector collector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(clusterConnectivity)
                .localNodeFileSystem(Mockito.mock(FileSystemStats.class))
                .build();

        ClusterState clusterState = collector.collectClusterState(
                 ImmutableList.of(), SequencerMetrics.UNKNOWN
        );

        NodeState localNodeState = clusterState.getNode(localEndpoint).get();
        NodeConnectivity localNodeConnectivity = localNodeState.getConnectivity();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeConnectivity.getConnectedNodes()).containsExactly(localEndpoint, B, C);
        assertThat(localNodeConnectivity.getFailedNodes()).isEmpty();

        NodeState nodeBState = clusterState.getNode(B).get();
        NodeConnectivity nodeBConnectivity = nodeBState.getConnectivity();
        assertThat(nodeBConnectivity.getConnectedNodes()).containsExactly(A, B);

        NodeState nodeCState = clusterState.getNode(C).get();
        assertThat(nodeCState.isConnected()).isFalse();
    }

    /**
     * Check that the collector can collect wrong epochs from the cluster info classes.
     */
    @Test
    public void testWrongEpochs() {
        final String localEndpoint = A;

        Map<String, CompletableFuture<NodeState>> clusterConnectivity = new HashMap<>();

        final int newEpoch = 10;
        CompletableFuture<NodeState> wrongEpochCf = new CompletableFuture<>();
        wrongEpochCf.completeExceptionally(new WrongEpochException(newEpoch));

        CompletableFuture<NodeState> nodeAState = CompletableFuture.completedFuture(
                NodeState.getUnavailableNodeState(C)
        );

        clusterConnectivity.put(localEndpoint, nodeAState);
        clusterConnectivity.put(C, wrongEpochCf);

        ClusterStateCollector collector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(clusterConnectivity)
                .localNodeFileSystem(Mockito.mock(FileSystemStats.class))
                .build();

        ImmutableMap<String, Long> wrongEpochs = collector.collectWrongEpochs();
        assertThat(wrongEpochs.size()).isOne();
        assertThat(wrongEpochs.get(C)).isEqualTo(newEpoch);
    }

}
