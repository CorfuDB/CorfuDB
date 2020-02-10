package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.A;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.B;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.C;
import static org.corfudb.infrastructure.management.failuredetector.ClusterGraph.ClusterGraphHelper.cluster;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.connectivity;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.unavailable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterGraphTest {

    @Test
    public void testTransformation() {
        NodeState a = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(connectivity(A, ImmutableMap.of(A, OK, B, OK, C, FAILED)))
                .build();

        NodeState b = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(connectivity(B, ImmutableMap.of(A, OK, B, OK, C, FAILED)))
                .build();

        NodeState c = unavailableNodeState(C);

        ImmutableMap<String, NodeState> nodes = ImmutableMap.of(A, a, B, b, C, c);
        ClusterState clusterState = ClusterState.builder()
                .localEndpoint(A)
                .nodes(nodes)
                .unresponsiveNodes(ImmutableList.of())
                .build();

        ClusterGraph graph = ClusterGraph.toClusterGraph(clusterState);

        assertEquals(graph.size(), nodes.size());
    }

    @Test
    public void testToSymmetricForTwoNodes() {
        NodeConnectivity a = connectivity(A, ImmutableMap.of(A, OK, B, FAILED));
        NodeConnectivity b = connectivity(B, ImmutableMap.of(A, OK, B, OK));

        ClusterGraph graph = cluster(A, ImmutableList.of(), a, b);
        ClusterGraph symmetric = graph.toSymmetric();

        assertEquals(FAILED, graph.getNodeConnectivity(A).getConnectionStatus(B));
        assertEquals(OK, graph.getNodeConnectivity(B).getConnectionStatus(A));

        assertEquals(FAILED, symmetric.getNodeConnectivity(A).getConnectionStatus(B));
        assertEquals(FAILED, symmetric.getNodeConnectivity(B).getConnectionStatus(A));
    }

    @Test
    public void testToSymmetricForThreeNodes() {
        NodeConnectivity a = connectivity(A, ImmutableMap.of(A, OK, B, FAILED, C, FAILED));
        NodeConnectivity b = connectivity(B, ImmutableMap.of(A, OK, B, OK, C, OK));
        NodeConnectivity c = connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK));

        ClusterGraph symmetric = cluster(A, ImmutableList.of(), a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity(B).getConnectionStatus(A));
        assertEquals(FAILED, symmetric.getNodeConnectivity(C).getConnectionStatus(A));

        symmetric = cluster(B, ImmutableList.of(), a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity(B).getConnectionStatus(A));
        assertEquals(FAILED, symmetric.getNodeConnectivity(C).getConnectionStatus(A));

        symmetric = cluster(C, ImmutableList.of(), a, b, c).toSymmetric();
        assertEquals(FAILED, symmetric.getNodeConnectivity(B).getConnectionStatus(A));
        assertEquals(FAILED, symmetric.getNodeConnectivity(C).getConnectionStatus(A));
    }

    @Test
    public void testToSymmetricForThreeNodesWithUnavailableNodeB() {
        NodeConnectivity a = connectivity(A, ImmutableMap.of(A, OK, B, FAILED, C, FAILED));
        NodeConnectivity b = unavailable(B);
        NodeConnectivity c = connectivity(C, ImmutableMap.of(A, OK, B, OK, C, OK));

        ClusterGraph graph = cluster(A, ImmutableList.of(), a, b, c);
        ClusterGraph symmetric = graph.toSymmetric();

        NodeConnectivity symmetricNodeA = symmetric.getNodeConnectivity(A);
        assertEquals(OK, symmetricNodeA.getConnectionStatus(A));
        assertEquals(FAILED, symmetricNodeA.getConnectionStatus(B));
        assertEquals(FAILED, symmetricNodeA.getConnectionStatus(C));

        assertThatThrownBy(() -> symmetric.getNodeConnectivity(B).getConnectionStatus(A))
                .isInstanceOf(IllegalStateException.class);

        assertEquals(FAILED, symmetric.getNodeConnectivity(C).getConnectionStatus(A));
        assertEquals(OK, symmetric.getNodeConnectivity(C).getConnectionStatus(B));
        assertEquals(OK, symmetric.getNodeConnectivity(C).getConnectionStatus(C));
    }

    @Test
    public void testDecisionMaker() {
        NodeConnectivity a = connectivity(A, ImmutableMap.of(A, OK, B, OK, C, OK));
        NodeConnectivity b = connectivity(B, ImmutableMap.of(A, FAILED, B, OK, C, OK));
        NodeConnectivity c = connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK));

        ClusterGraph graph = cluster(A, ImmutableList.of(), a, b, c);
        Optional<NodeRank> decisionMaker = graph.toSymmetric().getDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(decisionMaker.get(), new NodeRank(A, 2));
    }

    @Test
    public void testFailedNode() {
        ClusterGraph graph = cluster(
                A,
                ImmutableList.of(),
                connectivity(A, ImmutableMap.of(A, OK, B, OK, C, OK)),
                connectivity(B, ImmutableMap.of(A, FAILED, B, OK, C, OK)),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );

        Optional<NodeRank> failedNode = graph.toSymmetric().findFailedNode();
        assertTrue(failedNode.isPresent());
        assertEquals(failedNode.get(), new NodeRank(B, 1));

        graph = cluster(
                A,
                ImmutableList.of(B),
                connectivity(A, ImmutableMap.of(A, OK, B, OK, C, OK)),
                connectivity(B, ImmutableMap.of(A, OK, B, OK, C, FAILED)),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );
        failedNode = graph.toSymmetric().findFailedNode();
        assertFalse(failedNode.isPresent());

        graph = cluster(
                A,
                ImmutableList.of(),
                connectivity(A, ImmutableMap.of(A, OK, B, OK, C, OK)),
                connectivity(B, ImmutableMap.of(A, OK, B, OK, C, FAILED)),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );
        failedNode = graph.toSymmetric().findFailedNode();
        assertTrue(failedNode.isPresent());
        assertEquals(failedNode.get(), new NodeRank(C, 2));
    }

    @Test
    public void testFindFullyConnectedResponsiveNodes() {
        ClusterGraph graph = cluster(
                A,
                ImmutableList.of(B),
                connectivity(A, ImmutableMap.of(A, OK, B, FAILED, C, OK)),
                unavailable(B),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );
        graph = graph.toSymmetric();

        Optional<NodeRank> responsiveNode = graph.findFullyConnectedNode(C);
        assertTrue(responsiveNode.isPresent());
        assertEquals(new NodeRank(C, 2), responsiveNode.get());

        graph = cluster(
                A,
                ImmutableList.of(C),
                connectivity(A, ImmutableMap.of(A, OK, B, FAILED, C, OK)),
                unavailable(B),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );

        responsiveNode = graph.toSymmetric().findFullyConnectedNode(C);

        assertTrue(responsiveNode.isPresent());
        assertEquals(C, responsiveNode.get().getEndpoint());
        assertEquals(2, responsiveNode.get().getNumConnections());

        graph = cluster(
                A,
                ImmutableList.of(B),
                connectivity(A, ImmutableMap.of(A, OK, B, FAILED, C, OK)),
                connectivity(B, ImmutableMap.of(A, FAILED, B, OK, C, FAILED)),
                connectivity(C, ImmutableMap.of(A, OK, B, FAILED, C, OK))
        );
        graph = graph.toSymmetric();

        responsiveNode = graph.findFullyConnectedNode(C);

        assertTrue(responsiveNode.isPresent());
        assertEquals(new NodeRank(C, 2), responsiveNode.get());
    }

    private NodeState unavailableNodeState(String endpoint) {
        return new NodeState(
                unavailable(endpoint),
                SequencerMetrics.UNKNOWN
        );
    }
}