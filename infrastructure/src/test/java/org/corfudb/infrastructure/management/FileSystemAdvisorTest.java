package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemAdvisorTest {

    @Test
    public void noHealedServer() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;
        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, FAILED),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), FAILED, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeHealedNode = advisor.healedServer(cluster);

        assertFalse(maybeHealedNode.isPresent());
    }

    @Test
    public void healedServer() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;
        PartitionAttributeStats attributes = new PartitionAttributeStats(false, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, FAILED),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), FAILED, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeHealedNode = advisor.healedServer(cluster);

        assertTrue(maybeHealedNode.isPresent());
        assertEquals(new NodeRankByPartitionAttributes(localEndpoint, attributes), maybeHealedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributes() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes);

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
        assertEquals(new NodeRankByPartitionAttributes(NodeNames.A, attributes), maybeFailedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributesIsUnresponsive() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        assertFalse(maybeFailedNode.isPresent());
    }
}
