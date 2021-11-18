package org.corfudb.infrastructure.management.failuredetector;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.CompleteGraphAdvisor;
import org.corfudb.infrastructure.management.NodeStateTestUtil;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByResourceQuota;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DecisionMakerAgentTest {

    @Test
    void testFindResourceQuotaDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        FileSystemStats exceededQuotaFsStats = NodeStateTestUtil.buildExceededQuota();
        ResourceQuotaStats regularQuotaStats = NodeStateTestUtil.buildRegularQuota();
        FileSystemStats regularQuotaFsStats = new FileSystemStats(
                regularQuotaStats, Mockito.mock(PartitionAttributeStats.class)
        );

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(NodeNames.A, epoch, Optional.of(exceededQuotaFsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(exceededQuotaFsStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(regularQuotaFsStats), OK, OK, OK)
        );

        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, Mockito.mock(ClusterAdvisor.class));
        Optional<NodeRankByResourceQuota> quotaDecisionMaker = decisionMakerAgent.findResourceQuotaDecisionMaker();
        assertTrue(quotaDecisionMaker.isPresent());
        assertEquals(NodeNames.C, quotaDecisionMaker.get().getEndpoint());
    }

    @Test
    void testFindPartitionAttributesDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats readOnlyFsStats = new FileSystemStats(
                NodeStateTestUtil.buildRegularQuota(),
                new PartitionAttributeStats(true, 0, 0)
        );

        FileSystemStats writableFsStats = new FileSystemStats(
                NodeStateTestUtil.buildRegularQuota(),
                new PartitionAttributeStats(false, 0, 0)
        );

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, Optional.of(writableFsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(readOnlyFsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, Optional.of(readOnlyFsStats), OK, OK, OK)
        );

        DecisionMakerAgent decisionMakerAgent = new DecisionMakerAgent(cluster, Mockito.mock(ClusterAdvisor.class));
        Optional<NodeRank.NodeRankByPartitionAttributes> quotaDecisionMaker = decisionMakerAgent
                .findPartitionAttributesDecisionMaker();

        assertTrue(quotaDecisionMaker.isPresent());
        assertEquals(localEndpoint, quotaDecisionMaker.get().getEndpoint());
    }

    @Test
    void testFindDecisionMaker() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats readOnlyFsStats = new FileSystemStats(
                NodeStateTestUtil.buildRegularQuota(),
                new PartitionAttributeStats(true, 0, 0)
        );

        FileSystemStats writableFsStats = new FileSystemStats(
                NodeStateTestUtil.buildRegularQuota(),
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
}