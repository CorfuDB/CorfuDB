package org.corfudb.infrastructure.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
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
                NodeState.getDefaultNodeState("c")
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
     * Can't make a decision.
     */
    @Test
    public void testFailedServer_allDisconnected_from_b_perspective() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                NodeState.getDefaultNodeState("a"),
                nodeState("b", true, true, true),
                NodeState.getDefaultNodeState("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers, "a");
        assertFalse(failedServer.isPresent());
    }

    private NodeState nodeState(String endpoint, boolean... connectionStates) {
        Map<String, Boolean> connectivity = new HashMap<>();
        for (int i = 0; i < connectionStates.length; i++) {
            connectivity.put(NODE_NAMES.get(i), connectionStates[i]);
        }

        NodeConnectivity nodeConnectivity = NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeState.NodeConnectivityState.CONNECTED)
                .connectivity(ImmutableMap.copyOf(connectivity))
                .build();

        return NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new NodeState.HeartbeatTimestamp(0, 0))
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
}