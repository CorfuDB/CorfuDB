package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByResourceQuota;
import org.junit.jupiter.api.Test;

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

        final int limit = 100;
        final int used = 200;
        FileSystemStats fsStats = new FileSystemStats(new ResourceQuotaStats(limit, used));

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState("b", epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState("c", epoch, OK, OK, OK)
        );


        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByResourceQuota> maybeFailedNode = advisor.findFailedNodeByResourceQuota(cluster);

        assertTrue(maybeFailedNode.isPresent());
        assertEquals("b", maybeFailedNode.get().getEndpoint());
    }

    @Test
    public void testHealedServer() {
        final long epoch = 0;
        final String localEndpoint = "c";
        final int limit = 100;
        final int used = 80;

        ResourceQuotaStats quota = new ResourceQuotaStats(limit, used);
        FileSystemStats fsStats = new FileSystemStats(quota);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState("a", epoch, OK, OK, FAILED),
                nodeState("b", epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), FAILED, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByResourceQuota> maybeHealedNode = advisor.healedServer(cluster);

        assertTrue(maybeHealedNode.isPresent());
        assertEquals(new NodeRankByResourceQuota(localEndpoint, quota), maybeHealedNode.get());
    }
}