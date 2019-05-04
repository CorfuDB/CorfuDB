package org.corfudb.infrastructure.management;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.management.failuredetector.ClusterGraph.ClusterGraphHelper.cluster;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.connectivity;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.unavailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

public class ClusterGraphTest {

    @Test
    public void testTransformation() {
        NodeState a = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("a", ImmutableMap.of("a", OK, "b", OK, "c", FAILED)))
                .build();

        NodeState b = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(connectivity("b", ImmutableMap.of("a", OK, "b", OK, "c", FAILED)))
                .build();

        NodeState c = unavailableNodeState("c");

        ImmutableMap<String, NodeState> nodes = ImmutableMap.of("a", a, "b", b, "c", c);
        ClusterState clusterState = ClusterState.builder()
                .localEndpoint("a")
                .nodes(nodes)
                .build();

        ClusterGraph graph = ClusterGraph.toClusterGraph(clusterState, "a");

        assertEquals(graph.size(), nodes.size());
    }

    @Test
    public void testToSymmetricForTwoNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", OK, "b", FAILED));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", OK, "b", OK));

        ClusterGraph graph = cluster("a", a, b);
        ClusterGraph symmetric = graph.toSymmetric();

        assertEquals(FAILED, graph.getNodeConnectivity("a").getConnectionStatus("b"));
        assertEquals(OK, graph.getNodeConnectivity("b").getConnectionStatus("a"));

        assertEquals(FAILED, symmetric.getNodeConnectivity("a").getConnectionStatus("b"));
        assertEquals(FAILED, symmetric.getNodeConnectivity("b").getConnectionStatus("a"));
    }

    @Test
    public void testToSymmetricForThreeNodes() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", OK, "b", FAILED, "c", FAILED));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", OK, "b", OK, "c", OK));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK));

        ClusterGraph symmetric = cluster("a", a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity("b").getConnectionStatus("a"));
        assertEquals(FAILED, symmetric.getNodeConnectivity("c").getConnectionStatus("a"));

        symmetric = cluster("b", a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity("b").getConnectionStatus("a"));
        assertEquals(FAILED, symmetric.getNodeConnectivity("c").getConnectionStatus("a"));

        symmetric = cluster("c", a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity("b").getConnectionStatus("a"));
        assertEquals(FAILED, symmetric.getNodeConnectivity("c").getConnectionStatus("a"));
    }

    @Test
    public void testToSymmetricForThreeNodesWithUnavailableNodeB() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", OK, "b", FAILED, "c", FAILED));
        NodeConnectivity b = unavailable("b");
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", OK, "b", OK, "c", OK));

        ClusterGraph graph = cluster("a", a, b, c);
        ClusterGraph symmetric = graph.toSymmetric();

        NodeConnectivity symmetricNodeA = symmetric.getNodeConnectivity("a");
        assertEquals(OK, symmetricNodeA.getConnectionStatus("a"));
        assertEquals(FAILED, symmetricNodeA.getConnectionStatus("b"));
        assertEquals(FAILED, symmetricNodeA.getConnectionStatus("c"));

        assertThatThrownBy(() -> symmetric.getNodeConnectivity("b").getConnectionStatus("a"))
                .isInstanceOf(IllegalStateException.class);

        assertEquals(FAILED, symmetric.getNodeConnectivity("c").getConnectionStatus("a"));
        assertEquals(OK, symmetric.getNodeConnectivity("c").getConnectionStatus("b"));
        assertEquals(OK, symmetric.getNodeConnectivity("c").getConnectionStatus("c"));
    }

    @Test
    public void testDecisionMaker() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", OK, "b", OK, "c", OK));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", FAILED, "b", OK, "c", OK));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK));

        ClusterGraph graph = cluster("a", a, b, c);
        Optional<NodeRank> decisionMaker = graph.toSymmetric().getDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(decisionMaker.get(), new NodeRank("a", 2));
    }

    @Test
    public void testFailedNode() {
        NodeConnectivity a = connectivity("a", ImmutableMap.of("a", OK, "b", OK, "c", OK));
        NodeConnectivity b = connectivity("b", ImmutableMap.of("a", FAILED, "b", OK, "c", OK));
        NodeConnectivity c = connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK));

        ClusterGraph graph = cluster("a", a, b, c);
        Optional<NodeRank> failedNode = graph.toSymmetric().findFailedNode();

        assertTrue(failedNode.isPresent());
        assertEquals(failedNode.get(), new NodeRank("b", 1));
    }

    @Test
    public void testFindFullyConnectedResponsiveNodes() {
        ClusterGraph graph = cluster(
                "a",
                connectivity("a", ImmutableMap.of("a", OK, "b", FAILED, "c", OK)),
                unavailable("b"),
                connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK))
        );
        graph = graph.toSymmetric();

        Optional<NodeRank> responsiveNode = graph.findFullyConnectedNode("c", Collections.singletonList("b"));
        assertTrue(responsiveNode.isPresent());
        assertEquals(new NodeRank("c", 2), responsiveNode.get());

        responsiveNode = graph.findFullyConnectedNode("c", Collections.singletonList("c"));

        assertTrue(responsiveNode.isPresent());
        assertEquals("c", responsiveNode.get().getEndpoint());
        assertEquals(2, responsiveNode.get().getNumConnections());

        graph = cluster(
                "a",
                connectivity("a", ImmutableMap.of("a", OK, "b", FAILED, "c", OK)),
                connectivity("b", ImmutableMap.of("a", FAILED, "b", OK, "c", FAILED)),
                connectivity("c", ImmutableMap.of("a", OK, "b", FAILED, "c", OK))
        );
        graph = graph.toSymmetric();

        responsiveNode = graph.findFullyConnectedNode("c", Collections.singletonList("b"));

        assertTrue(responsiveNode.isPresent());
        assertEquals(new NodeRank("c", 2), responsiveNode.get());
    }

    private NodeState unavailableNodeState(String endpoint) {
        return new NodeState(
                unavailable(endpoint),
                new HeartbeatTimestamp(Layout.INVALID_EPOCH, 0),
                SequencerMetrics.UNKNOWN
        );
    }
}