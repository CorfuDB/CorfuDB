package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.BatchProcessorStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DecisionMakerAgentTest {

    @Test
    public void testHealthyNodes() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        FileSystemStats writableFsStats = getWritableFsStats();
        FileSystemStats bpErrorStats = getFsStatsWithBatchProcessorError();
        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(bpErrorStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(readOnlyFsStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);

        Set<String> healthyNodes = decisionMakerAgent.healthyNodes();
        HashSet<String> expectedHealthyNodes = new HashSet<>();
        expectedHealthyNodes.add(localEndpoint);
        assertEquals(expectedHealthyNodes, healthyNodes);
    }

    @Test
    public void testFindDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();
        FileSystemStats writableFsStats = getWritableFsStats();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(readOnlyFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(writableFsStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);
        Optional<String> decisionMaker = decisionMakerAgent.findDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(localEndpoint, decisionMaker.get());
    }

    @Test
    public void testFindDecisionMaker_LocalNodeIsNotADecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();
        FileSystemStats writableFsStats = getWritableFsStats();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(readOnlyFsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(writableFsStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);
        Optional<String> decisionMaker = decisionMakerAgent.findDecisionMaker();

        assertFalse(decisionMaker.isPresent());
    }

    @Test
    public void testFullyConnectedDecisionMakerWithReadOnlyFileSystem() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();
        FileSystemStats writableFsStats = getWritableFsStats();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(readOnlyFsStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(writableFsStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);
        Optional<String> decisionMaker = decisionMakerAgent.findDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(localEndpoint, decisionMaker.get());
    }

    @Test
    public void testBpErrorWithADecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        FileSystemStats writableFsStats = getWritableFsStats();
        FileSystemStats bpErrorStats = getFsStatsWithBatchProcessorError();
        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(bpErrorStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(readOnlyFsStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);
        Optional<String> decisionMaker = decisionMakerAgent.findDecisionMaker();

        assertTrue(decisionMaker.isPresent());
        assertEquals(localEndpoint, decisionMaker.get());
    }

    @Test
    public void testBpErrorWithNoDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        FileSystemStats readOnlyFsStats = getReadOnlyFsStats();
        FileSystemStats bpErrorStats = getFsStatsWithBatchProcessorError();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(readOnlyFsStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(bpErrorStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(bpErrorStats), OK, OK, OK)
        );

        ClusterAdvisor clusterAdvisor = new CompleteGraphAdvisor(localEndpoint);
        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, clusterAdvisor);
        Optional<String> decisionMaker = decisionMakerAgent.findDecisionMaker();

        assertFalse(decisionMaker.isPresent());
    }

    private FileSystemStats getReadOnlyFsStats() {
        return new FileSystemStats(new PartitionAttributeStats(true, 0, 0), BatchProcessorStats.OK);
    }

    private FileSystemStats getWritableFsStats() {
        return new FileSystemStats(new PartitionAttributeStats(false, 0, 0), BatchProcessorStats.OK);
    }

    private FileSystemStats getFsStatsWithBatchProcessorError() {
        return new FileSystemStats(new PartitionAttributeStats(false, 0, 0), BatchProcessorStats.ERR);
    }
}
