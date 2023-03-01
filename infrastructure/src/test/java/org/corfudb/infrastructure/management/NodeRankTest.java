package org.corfudb.infrastructure.management;


import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.BatchProcessorStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.runtime.proto.FileSystemStats.BatchProcessorStatus;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.corfudb.infrastructure.NodeNames.A;
import static org.corfudb.infrastructure.NodeNames.B;
import static org.corfudb.infrastructure.NodeNames.C;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class NodeRankTest {

    @Test
    public void testSortingByNumberOfConnections() {
        NodeRank rank1 = new NodeRank(A, 1);
        NodeRank rank2 = new NodeRank(B, 2);
        SortedSet<NodeRank> ranks = new TreeSet<>(Arrays.asList(rank1, rank2));

        assertEquals(ranks.first(), rank2);
    }

    @Test
    public void testSortingByName() {
        NodeRank rank1 = new NodeRank(A, 1);
        NodeRank rank2 = new NodeRank(B, 1);
        SortedSet<NodeRank> ranks = new TreeSet<>(Arrays.asList(rank1, rank2));

        assertEquals(ranks.first(), rank1);
    }

    @Test
    public void testPartitionAttributesOrdering() {
        BatchProcessorStatus ok = BatchProcessorStatus.OK;
        NodeRankByPartitionAttributes attrA = buildAttributes(A, true, ok);
        NodeRankByPartitionAttributes attrB = buildAttributes(B, true, ok);
        NodeRankByPartitionAttributes attrC = buildAttributes(C, false, ok);

        TreeSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        set.add(attrA);
        set.add(attrC);
        set.add(attrB);

        assertEquals(attrC, set.pollFirst());
        assertEquals(attrA, set.pollFirst());
        assertEquals(attrB, set.pollFirst());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testPartitionAttributesBatchProcessorStatus() {
        BatchProcessorStatus ok = BatchProcessorStatus.OK;
        BatchProcessorStatus err = BatchProcessorStatus.ERROR;

        NodeRankByPartitionAttributes attrA = buildAttributes(A, false, err);
        NodeRankByPartitionAttributes attrB = buildAttributes(B, false, ok);
        NodeRankByPartitionAttributes attrC = buildAttributes(C, false, ok);

        TreeSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        set.add(attrA);
        set.add(attrC);
        set.add(attrB);

        assertEquals(attrB, set.pollFirst());
        assertEquals(attrC, set.pollFirst());
        assertEquals(attrA, set.pollFirst());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testPartitionAttributesTotalOrdering() {
        BatchProcessorStatus ok = BatchProcessorStatus.OK;
        BatchProcessorStatus err = BatchProcessorStatus.ERROR;

        NodeRankByPartitionAttributes attrA = buildAttributes(A, false, err);
        NodeRankByPartitionAttributes attrB = buildAttributes(NodeNames.B, true, ok);

        NodeRankByPartitionAttributes attrC = buildAttributes(C, false, ok);

        NodeRankByPartitionAttributes attrD = buildAttributes(NodeNames.D, false, err);
        NodeRankByPartitionAttributes attrE = buildAttributes(NodeNames.E, true, err);

        TreeSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        set.add(attrA);
        set.add(attrB);
        set.add(attrC);
        set.add(attrD);
        set.add(attrE);

        assertEquals(attrC, set.pollFirst());
        assertEquals(attrB, set.pollFirst());
        assertEquals(attrA, set.pollFirst());
        assertEquals(attrD, set.pollFirst());
        assertEquals(attrE, set.pollFirst());
        assertTrue(set.isEmpty());
    }

    private NodeRankByPartitionAttributes buildAttributes(String node, boolean readOnly, BatchProcessorStatus bpStatus) {
        PartitionAttributeStats partitionStats = new PartitionAttributeStats(readOnly, 0, 0);

        BatchProcessorStats bpStats = new BatchProcessorStats(bpStatus);
        FileSystemStats fsStats = new FileSystemStats(partitionStats, bpStats);

        return new NodeRankByPartitionAttributes(node, fsStats);
    }
}