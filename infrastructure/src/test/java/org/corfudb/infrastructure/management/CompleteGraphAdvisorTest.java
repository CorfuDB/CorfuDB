package org.corfudb.infrastructure.management;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
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

    @Test
    public void testFailedServer_disconnected_c() {
        final String localEndpoint = "a";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                nodeState("a", OK, OK, FAILED),
                nodeState("b", OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        List<String> unresponsiveServers = new ArrayList<>();
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, unresponsiveServers);
        assertTrue(failedServer.isPresent());
        assertEquals(new NodeRank("c", 0), failedServer.get());
    }

    @Test
    public void testFailedServer_asymmetricFailureBetween_b_c() {
        final String localEndpoint = "a";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
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
        final String localEndpoint = "b";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                NodeState.getUnavailableNodeState("a"),
                nodeState("b", OK, OK, OK),
                NodeState.getUnavailableNodeState("c")
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
                "a",
                nodeState("a", OK, OK, OK),
                nodeState("b", OK, OK, FAILED),
                nodeState("c", OK, FAILED, OK)
        );

        ClusterState nodeBClusterState = buildClusterState(
                "b",
                nodeState("a", OK, OK, OK),
                nodeState("b", OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );
        ClusterState nodeCClusterState = buildClusterState(
                "c",
                nodeState("a", OK, OK, OK),
                NodeState.getUnavailableNodeState("b"),
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
        final String localEndpoint = "c";
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor(localEndpoint);

        ClusterState clusterState = buildClusterState(
                localEndpoint,
                nodeState("a", OK, FAILED, OK),
                nodeState("b", FAILED, OK, OK),
                nodeState("c", OK, OK, OK)
        );

        List<String> unresponsiveServers = new ArrayList<>();
        unresponsiveServers.add(localEndpoint);

        Optional<NodeRank> healedServer = advisor.healedServer(clusterState, unresponsiveServers);
        assertTrue(healedServer.isPresent());
        assertEquals(new NodeRank(localEndpoint, clusterState.size()), healedServer.get());
    }
}