package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DecisionMakerAgentTest {

    @Test
    public void testFindDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats readOnlyFsStats = new FileSystemStats(
                new PartitionAttributeStats(true, 0, 0)
        );

        FileSystemStats writableFsStats = new FileSystemStats(
                new PartitionAttributeStats(false, 0, 0)
        );

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
    public void testFullyConnectedDecisionMakerWithReadOnlyFileSystem() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        FileSystemStats readOnlyFsStats = new FileSystemStats(
                new PartitionAttributeStats(true, 0, 0)
        );

        FileSystemStats writableFsStats = new FileSystemStats(
                new PartitionAttributeStats(false, 0, 0)
        );

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
}
