package org.corfudb.infrastructure.management;

import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompleteGraphAdvisorTest {
    private static final List<String> NODE_NAMES = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k");

    @Test
    public void testFailedServer_disconnected_c() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor("a");

        ClusterState clusterState = buildClusterState(
                nodeState("a", OK, OK, FAILED),
                nodeState("b", OK, OK, FAILED),
                unavailable("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 0), failedServer.get());
    }

    @Test
    public void testFailedServer_asymmetricFailureBetween_b_c() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor("a");

        ClusterState clusterState = buildClusterState(
                nodeState("a", OK, OK, OK),
                nodeState("b", OK, OK, FAILED),
                nodeState("c", OK, FAILED, OK)
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 2), failedServer.get());
    }

    /**
     * B node believes that everyone disconnected, but actually it's B disconnected.
     * Make a decision to exclude C.
     */
    @Test
    public void testFailedServer_allDisconnected_from_b_perspective() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor("b");

        ClusterState clusterState = buildClusterState(
                unavailable("a"),
                nodeState("b", OK, OK, OK),
                unavailable("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 0), failedServer.get());
    }

    /**
     * Asymmetric partition where:
     * - B and C nodes disconnected from each other
     * - decision maker is node A
     * - node A makes node C unresponsive
     */
    @Test
    public void testFailureDetectionForThreeNodes_asymmetricPartition_b_c_disconnectedFromEachOther() {
        CompleteGraphAdvisor nodeAAdvisor = new CompleteGraphAdvisor("a");
        CompleteGraphAdvisor nodeBAdvisor = new CompleteGraphAdvisor("b");
        CompleteGraphAdvisor nodeCAdvisor = new CompleteGraphAdvisor("c");

        ClusterState nodeAClusterState = buildClusterState(
                nodeState("a", OK, OK, OK),
                nodeState("b", OK, OK, FAILED),
                nodeState("c", OK, FAILED, OK)
        );

        ClusterState nodeBClusterState = buildClusterState(
                nodeState("a", OK, OK, OK),
                nodeState("b", OK, OK, FAILED),
                unavailable("c")
        );
        ClusterState nodeCClusterState = buildClusterState(
                nodeState("a", OK, OK, OK),
                unavailable("b"),
                nodeState("c", OK, FAILED, OK)
        );

        List<String> unresponsiveServers = new ArrayList<>();

        //Node A is a decision maker, it excludes node C from the cluster
        Optional<NodeRank> nodeAFailedServer = nodeAAdvisor.failedServer(nodeAClusterState, unresponsiveServers);
        assertTrue(nodeAFailedServer.isPresent());
        assertEquals(new NodeRank("c", 2), nodeAFailedServer.get());

        //Node B knows that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeBFailedServer = nodeBAdvisor.failedServer(nodeBClusterState, unresponsiveServers);
        assertFalse(nodeBFailedServer.isPresent());

        //Node C know that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeCFailedServer = nodeCAdvisor.failedServer(nodeCClusterState, unresponsiveServers);
        assertFalse(nodeCFailedServer.isPresent());
    }

    @Test
    public void testHealedServer() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor("c");

        ClusterState clusterState = buildClusterState(
                nodeState("a", OK, FAILED, OK),
                nodeState("b", FAILED, OK, OK),
                nodeState("c", OK, OK, OK)
        );

        List<String> unresponsiveServers = new ArrayList<>();
        unresponsiveServers.add("c");

        Optional<NodeRank> healedServer = advisor.healedServer(clusterState, unresponsiveServers);
        assertTrue(healedServer.isPresent());
        assertEquals(new NodeRank("c", clusterState.size()), healedServer.get());
    }

    private NodeState nodeState(String endpoint, ConnectionStatus... connectionStates) {
        Map<String, ConnectionStatus> connectivity = new HashMap<>();
        for (int i = 0; i < connectionStates.length; i++) {
            connectivity.put(NODE_NAMES.get(i), connectionStates[i]);
        }

        NodeConnectivity nodeConnectivity = NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.CONNECTED)
                .connectivity(ImmutableMap.copyOf(connectivity))
                .build();

        return NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(nodeConnectivity)
                .build();
    }

    private ClusterState buildClusterState(NodeState... states) {
        Map<String, NodeState> graph = Arrays.stream(states)
                .collect(Collectors.toMap(state -> state.getConnectivity().getEndpoint(), Function.identity()));

        return ClusterState.builder()
                .nodes(ImmutableMap.copyOf(graph))
                .build();
    }

    private NodeState unavailable(String endpoint) {
        NodeConnectivity connectivity = NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.UNAVAILABLE)
                .connectivity(ImmutableMap.of())
                .build();

        return new NodeState(
                connectivity,
                new HeartbeatTimestamp(Layout.INVALID_EPOCH, 0),
                SequencerMetrics.UNKNOWN
        );
    }
}