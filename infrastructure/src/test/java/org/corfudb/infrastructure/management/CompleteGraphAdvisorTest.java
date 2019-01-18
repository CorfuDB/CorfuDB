package org.corfudb.infrastructure.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
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
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                nodeState("a", true, true, false),
                nodeState("b", true, true, false),
                unavailable("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers, "a");
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 0), failedServer.get());
    }

    @Test
    public void testFailedServer_asymmetricFailureBetween_b_c() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                nodeState("a", true, true, true),
                nodeState("b", true, true, false),
                nodeState("c", true, false, true)
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers, "a");
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 2), failedServer.get());
    }

    /**
     * B node believes that everyone disconnected, but actually it's B disconnected.
     * Make a decision to exclude C.
     */
    @Test
    public void testFailedServer_allDisconnected_from_b_perspective() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                unavailable("a"),
                nodeState("b", true, true, true),
                unavailable("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers, "b");
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
        CompleteGraphAdvisor nodeAAdvisor = new CompleteGraphAdvisor();
        CompleteGraphAdvisor nodeBAdvisor = new CompleteGraphAdvisor();
        CompleteGraphAdvisor nodeCAdvisor = new CompleteGraphAdvisor();

        ClusterState nodeAClusterState = buildClusterState(
                nodeState("a", true, true, true),
                nodeState("b", true, true, false),
                nodeState("c", true, false, true)
        );

        ClusterState nodeBClusterState = buildClusterState(
                nodeState("a", true, true, true),
                nodeState("b", true, true, false),
                unavailable("c")
        );
        ClusterState nodeCClusterState = buildClusterState(
                nodeState("a", true, true, true),
                unavailable("b"),
                nodeState("c", true, false, true)
        );

        List<String> unresponsiveServers = new ArrayList<>();

        //Node A is a decision maker, it excludes node C from the cluster
        Optional<NodeRank> nodeAFailedServer = nodeAAdvisor.failedServer(nodeAClusterState, unresponsiveServers, "a");
        assertTrue(nodeAFailedServer.isPresent());
        assertEquals(new NodeRank("c", 2), nodeAFailedServer.get());

        //Node B knows that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeBFailedServer = nodeBAdvisor.failedServer(nodeBClusterState, unresponsiveServers, "b");
        assertFalse(nodeBFailedServer.isPresent());

        //Node C know that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeCFailedServer = nodeCAdvisor.failedServer(nodeCClusterState, unresponsiveServers, "c");
        assertFalse(nodeCFailedServer.isPresent());
    }

    @Test
    public void testHealedServer() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                nodeState("a", true, false, true),
                nodeState("b", false, true, true),
                nodeState("c", true, true, true)
        );

        List<String> unresponsiveServers = new ArrayList<>();
        unresponsiveServers.add("c");

        Optional<NodeRank> healedServer = advisor.healedServer(clusterState, unresponsiveServers, "c");
        assertTrue(healedServer.isPresent());
        assertEquals(new NodeRank("c", clusterState.size()), healedServer.get());
    }

    private NodeState nodeState(String endpoint, boolean... connectionStates) {
        Map<String, Boolean> connectivity = new HashMap<>();
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