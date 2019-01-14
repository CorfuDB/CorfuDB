package org.corfudb.infrastructure.management;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.NodeState.NodeConnectivityState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterGraphTest {

    @Test
    public void testTransformation() {
        NodeState a = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("a", ImmutableMap.of("a", true, "b", true, "c", false)))
                .build();

        NodeState b = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("b", ImmutableMap.of("a", true, "b", true, "c", false)))
                .build();

        NodeState c = unavailableNodeState("c");

        ImmutableMap<String, NodeState> nodes = ImmutableMap.of("a", a, "b", b, "c", c);
        ClusterState clusterState = ClusterState.builder()
                .nodes(nodes)
                .build();

        ClusterGraph graph = ClusterGraph.transform(clusterState);

        assertEquals(graph.size(), nodes.size());
    }

    @Test
    public void testToSymmetricForTwoNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", true, "b", true));

        ClusterGraph graph = cluster(a, b);
        ClusterGraph symmetric = graph.toSymmetric();

        assertFalse(graph.getNode("a").getConnectionStatus("b"));
        assertTrue(graph.getNode("b").getConnectionStatus("a"));

        assertFalse(symmetric.getNode("a").getConnectionStatus("b"));
        assertFalse(symmetric.getNode("b").getConnectionStatus("a"));
    }

    @Test
    public void testToSymmetricForThreeNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false, "c", false));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);
        ClusterGraph symmetric = graph.toSymmetric();

        assertFalse(symmetric.getNode("b").getConnectionStatus("a"));
        assertFalse(symmetric.getNode("c").getConnectionStatus("a"));
    }

    @Test
    public void testToSymmetricForThreeNodesWithUnavailableNodeB() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", false, "c", false));
        NodeConnectivity b = unavailable("b");
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);
        ClusterGraph symmetric = graph.toSymmetric();

        assertTrue(symmetric.getNode("a").getConnectionStatus("a"));
        assertFalse(symmetric.getNode("a").getConnectionStatus("b"));
        assertFalse(symmetric.getNode("a").getConnectionStatus("c"));

        assertFalse(symmetric.getNode("b").getConnectionStatus("a"));
        assertFalse(symmetric.getNode("b").getConnectionStatus("b"));
        assertFalse(symmetric.getNode("b").getConnectionStatus("c"));

        assertFalse(symmetric.getNode("c").getConnectionStatus("a"));
        assertFalse(symmetric.getNode("c").getConnectionStatus("b"));
        assertTrue(symmetric.getNode("c").getConnectionStatus("c"));
    }

    @Test
    public void testDecisionMaker() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", false, "b", true, "c", true));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);
        Optional<NodeRank> decisionMaker = graph.toSymmetric().getDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(decisionMaker.get(), new NodeRank("a", 2));
    }

    @Test
    public void testFailedNode() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", false, "b", true, "c", true));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", true, "b", false, "c", true));

        ClusterGraph graph = cluster(a, b, c);
        Optional<NodeRank> failedNode = graph.toSymmetric().findFailedNode();

        assertTrue(failedNode.isPresent());
        assertEquals(failedNode.get(), new NodeRank("b", 1));
    }

    @Test
    public void testFindFullyConnectedResponsiveNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", true, "b", true, "c", false));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", true, "b", true, "c", true));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", false, "b", true, "c", true));

        ClusterGraph graph = cluster(a, b, c).toSymmetric();

        ImmutableMap<String, NodeRank> responsiveNodes = graph.findFullyConnectedResponsiveNodes(
                Collections.singletonList("b")
        );

        assertEquals(responsiveNodes.size(), 1);
        assertEquals(responsiveNodes.get("b").getEndpoint(), "b");
        assertEquals(responsiveNodes.get("b").getNumConnections(), graph.size());
    }

    private ClusterGraph cluster(NodeConnectivity... nodes) {
        Map<String, NodeConnectivity> graph = Arrays.stream(nodes)
                .collect(Collectors.toMap(NodeConnectivity::getEndpoint, Function.identity()));

        return ClusterGraph.builder()
                .graph(ImmutableMap.copyOf(graph))
                .build();
    }

    private NodeConnectivity connectivity(String endpoint, ImmutableMap<String, Boolean> state) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityState.CONNECTED)
                .connectivity(state)
                .build();
    }

    private NodeConnectivity unavailable(String endpoint) {
        return NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityState.UNAVAILABLE)
                .connectivity(ImmutableMap.of())
                .build();
    }

    private NodeState unavailableNodeState(String endpoint) {
        return new NodeState(
                unavailable(endpoint),
                new NodeState.HeartbeatTimestamp(Layout.INVALID_EPOCH, 0),
                SequencerMetrics.UNKNOWN
        );
    }
}