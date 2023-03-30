package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.BasicTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.ParallelTransferProcessor;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.clients.LogUnitClient;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;

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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

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
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(B),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, FAILED),
                nodeState(C, epoch, OK, FAILED, OK)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertFalse(failedServer.isPresent());
    }

    @Test
    public void testFailedServer_disconnected_c() {
        final String localEndpoint = A;
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, OK, FAILED),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank(C, 0), failedServer.get());
    }

    @Test
    public void testFailedServer_asymmetricFailureBetween_b_c() {
        final String localEndpoint = A;
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, FAILED),
                nodeState(C, epoch, OK, FAILED, OK)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank(C, 2), failedServer.get());
    }

    /**
     * B node believes that everyone disconnected, but actually it's B disconnected.
     * Make a decision to exclude C.
     */
    @Test
    public void testFailedServer_allDisconnected_from_b_perspective() {
        final String localEndpoint = B;
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                NodeState.getUnavailableNodeState(A),
                nodeState(localEndpoint, epoch, OK, OK, OK),
                NodeState.getUnavailableNodeState(C)
        );

        Optional<NodeRank> failedServer = advisor.failedServer(clusterState);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank(C, 0), failedServer.get());
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
                A,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, FAILED),
                nodeState(C, epoch, OK, FAILED, OK)
        );

        ClusterState nodeBClusterState = buildClusterState(
                B,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, OK),
                nodeState(B, epoch, OK, OK, FAILED),
                NodeState.getUnavailableNodeState(C)
        );
        ClusterState nodeCClusterState = buildClusterState(
                C,
                ImmutableList.of(),
                nodeState(A, epoch, OK, OK, OK),
                NodeState.getUnavailableNodeState(B),
                nodeState(C, epoch, OK, FAILED, OK)
        );

        //Node A is a decision maker, it excludes node C from the cluster
        Optional<NodeRank> nodeAFailedServer = nodeAAdvisor.failedServer(nodeAClusterState);
        assertTrue(nodeAFailedServer.isPresent());
        assertEquals(new NodeRank(C, 2), nodeAFailedServer.get());

        //Node B can detect that node C has failed
        // (but in the end node C will be ignored by Node B because of a decision maker)
        Optional<NodeRank> nodeBFailedServer = nodeBAdvisor.failedServer(nodeBClusterState);
        assertTrue(nodeBFailedServer.isPresent());

        //Node C will detect its own failure
        //(but in the end node this failure will be ignored because of a decision maker)
        Optional<NodeRank> nodeCFailedServer = nodeCAdvisor.failedServer(nodeCClusterState);
        assertTrue(nodeCFailedServer.isPresent());
    }

    @Test
    public void testHealedServer() {
        final String localEndpoint = C;
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(A, epoch, OK, FAILED, OK),
                nodeState(B, epoch, FAILED, OK, OK),
                nodeState(localEndpoint, epoch, OK, OK, OK)
        );

        Optional<NodeRank> healedServer = advisor.healedServer(clusterState, localEndpoint);
        assertTrue(healedServer.isPresent());
        assertEquals(new NodeRank(localEndpoint, clusterState.size()), healedServer.get());
    }
}