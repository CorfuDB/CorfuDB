package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.BatchProcessorStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.C;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.ClusterState.buildClusterState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemAdvisorTest {

    @Test
    public void noHealedServerReadOnlyPartition() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;
        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.OK);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeHealedNode = advisor.healedServer(cluster);

        assertFalse(maybeHealedNode.isPresent());
    }

    @Test
    public void noHealedServerBpError() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;
        PartitionAttributeStats attributes = new PartitionAttributeStats(false, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.ERR);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
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
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.OK);

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

        NodeRankByPartitionAttributes expected = new NodeRankByPartitionAttributes(localEndpoint, fsStats);
        assertEquals(expected, maybeHealedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributes() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.OK);

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
        assertEquals(new NodeRankByPartitionAttributes(NodeNames.A, fsStats), maybeFailedNode.get());
    }

    @Test
    public void emptyPartitionAttributes() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        assertFalse(maybeFailedNode.isPresent());
    }

    @Test
    public void failedNodeByPartitionAttributesReadOnlyAndBpError() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.B;

        PartitionAttributeStats readOnlyAttr = new PartitionAttributeStats(true, 100, 200);

        FileSystemStats fsStatsBpError = new FileSystemStats(readOnlyAttr, BatchProcessorStats.ERR);
        FileSystemStats fsStatsOk = new FileSystemStats(readOnlyAttr, BatchProcessorStats.OK);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState(A, epoch, Optional.of(fsStatsBpError), OK, OK, FAILED),
                nodeState(localEndpoint, epoch, Optional.of(fsStatsOk), OK, OK, OK),
                nodeState(C, epoch, Optional.of(fsStatsOk), FAILED, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        assertTrue(maybeFailedNode.isPresent());
        //We believe that batch processor failure is more important than read only file system because
        // batch processor failre should mean data corruption,
        // that's why node A marked as a failed node rather than node c.
        assertEquals(new NodeRankByPartitionAttributes(A, fsStatsBpError), maybeFailedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributesNodesABReadOnly() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.OK);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(NodeNames.B, epoch, Optional.of(fsStats), OK, OK, OK),
                nodeState(localEndpoint, epoch, OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        assertTrue(maybeFailedNode.isPresent());
        assertEquals(new NodeRankByPartitionAttributes(NodeNames.B, fsStats), maybeFailedNode.get());
    }

    @Test
    public void failedNodeByPartitionAttributesIsUnresponsive() {
        final long epoch = 0;
        final String localEndpoint = NodeNames.C;

        PartitionAttributeStats attributes = new PartitionAttributeStats(true, 100, 200);
        FileSystemStats fsStats = new FileSystemStats(attributes, BatchProcessorStats.OK);

        ClusterState cluster = buildClusterState(
                localEndpoint,
                ImmutableList.of(localEndpoint),
                nodeState(NodeNames.A, epoch, OK, OK, OK),
                nodeState(NodeNames.B, epoch, OK, OK, OK),
                nodeState(localEndpoint, epoch, Optional.of(fsStats), OK, OK, OK)
        );

        FileSystemAdvisor advisor = new FileSystemAdvisor();
        Optional<NodeRankByPartitionAttributes> maybeFailedNode = advisor.findFailedNodeByPartitionAttributes(cluster);

        //The node is already in the unresponsive list, we won't detect it as a failed node since it's already failed.
        assertFalse(maybeFailedNode.isPresent());
    }
}
