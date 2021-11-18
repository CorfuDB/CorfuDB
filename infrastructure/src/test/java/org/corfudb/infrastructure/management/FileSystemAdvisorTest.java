package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByResourceQuota;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemAdvisorTest {

    @Test
    void findFailedNodeByResourceQuota() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.A;

        FileSystemStats fsStats = NodeStateTestUtil.buildExceededQuota();

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.C, epoch, OK, OK, OK)
        );


        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByResourceQuota> maybeFailedNode = advisor.findFailedNodeByResourceQuota(cluster);

        assertTrue(maybeFailedNode.isPresent());
        assertEquals(NodeNames.B, maybeFailedNode.get().getEndpoint());
    }

    @Test
    public void healedServer() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;
        ResourceQuotaStats quota = NodeStateTestUtil.buildRegularQuota();
        FileSystemStats fsStats = new FileSystemStats(quota, Mockito.mock(PartitionAttributeStats.class));

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, FAILED),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), FAILED, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByResourceQuota> maybeHealedNode = advisor.healedServer(cluster);

        assertTrue(maybeHealedNode.isPresent());
        assertEquals(new NodeRankByResourceQuota(localEndpoint, quota), maybeHealedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributes() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        ResourceQuotaStats quota = Mockito.mock(ResourceQuotaStats.class);
        PartitionAttributeStats attrs = new PartitionAttributeStats(true, 0, 0);
        FileSystemStats fsStats = new FileSystemStats(quota, attrs);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        assertTrue(maybeFailedNode.isPresent());
        assertEquals(new NodeRankByPartitionAttributes(NodeNames.A, attrs), maybeFailedNode.get());
    }
}