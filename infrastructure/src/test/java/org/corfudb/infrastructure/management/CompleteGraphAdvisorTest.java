package org.corfudb.infrastructure.management;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.A;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.B;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.junit.Test;

import java.util.Optional;

public class CompleteGraphAdvisorTest {

    private final long epoch = 1;

    /**
     * By definition, a fully connected node can not be added to the unresponsive list.
     * Failed connection(s) between unresponsive and fully connected node(s)
     * can't be used to determine if a fully connected node is failed.
     */
    @Test
    public void testUnresponsiveAndFullyConnectedNode() {
        final String localEndpoint = A;
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of("b"),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, FAILED),
                nodeState(C, epoch, OK, FAILED, OK)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertFalse(failedServer.isPresent());
    }

    @Test
    public void testFailedServer_disconnected_c() {
        final String localEndpoint = "a";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, FAILED),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 0), failedServer.get());
    }

    @Test
    public void testFailedServer_asymmetricFailureBetween_b_c() {
        final String localEndpoint = "a";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, OK),
                nodeState("b", epoch, OK, OK, FAILED),
                nodeState("c", epoch, OK, FAILED, OK)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 2), failedServer.get());
    }

    /**
     * B node believes that everyone disconnected, but actually it's B disconnected.
     * Make a decision to exclude C.
     */
    @Test
    public void testFailedServer_allDisconnected_from_b_perspective() {
        final String localEndpoint = "b";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                NodeState.getUnavailableNodeState("a"),
                nodeState("b", epoch, OK, OK, OK),
                NodeState.getUnavailableNodeState("c")
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
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
                "a",
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, OK),
                nodeState("b", epoch, OK, OK, FAILED),
                nodeState("c", epoch, OK, FAILED, OK)
        );

        ClusterState nodeBClusterState = buildClusterState(
                "b",
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, OK),
                nodeState("b", epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );
        ClusterState nodeCClusterState = buildClusterState(
                "c",
                ImmutableList.of(),
                nodeState("a", epoch, OK, OK, OK),
                NodeState.getUnavailableNodeState("b"),
                nodeState("c", epoch, OK, FAILED, OK)
        );

        //Node A is a decision maker, it excludes node C from the cluster
        Optional<NodeRank> nodeAFailedServer = nodeAAdvisor.failedServer(nodeAClusterState);
        assertTrue(nodeAFailedServer.isPresent());
        assertEquals(new NodeRank("c", 2), nodeAFailedServer.get());

        //Node B knows that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeBFailedServer = nodeBAdvisor.failedServer(nodeBClusterState);
        assertFalse(nodeBFailedServer.isPresent());

        //Node C know that node A is a decision maker, so do nothing
        Optional<NodeRank> nodeCFailedServer = nodeCAdvisor.failedServer(nodeCClusterState);
        assertFalse(nodeCFailedServer.isPresent());
    }

    @Test
    public void testHealedServer() {
        final String localEndpoint = "c";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState("a", epoch, OK, FAILED, OK),
                nodeState("b", epoch, FAILED, OK, OK),
                nodeState("c", epoch, OK, OK, OK)
        );

        Optional<NodeRank> healedServer = advisor.healedServer(clusterState);
        assertTrue(healedServer.isPresent());
        assertEquals(new NodeRank(localEndpoint, clusterState.size()), healedServer.get());
    }
}