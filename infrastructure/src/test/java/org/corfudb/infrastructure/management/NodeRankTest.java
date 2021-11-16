package org.corfudb.infrastructure.management;


import org.corfudb.infrastructure.NodeNames;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByPartitionAttributes;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank.NodeRankByResourceQuota;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class NodeRankTest {
    private static final int ONE_HUNDRED = 100;
    private static final int TEN = 10;
    private static final int THOUSAND = 1000;

    @Test
    public void testSortingByNumberOfConnections(){
        NodeRank rank1 = new NodeRank("a", 1);
        NodeRank rank2 = new NodeRank("b", 2);
        SortedSet<NodeRank> ranks = new TreeSet<>(Arrays.asList(rank1, rank2));

        assertEquals(ranks.first(), rank2);
    }

    @Test
    public void testSortingByName(){
        NodeRank rank1 = new NodeRank("a", 1);
        NodeRank rank2 = new NodeRank("b", 1);
        SortedSet<NodeRank> ranks = new TreeSet<>(Arrays.asList(rank1, rank2));

        assertEquals(ranks.first(), rank1);
    }

    @Test
    public void testMinQuotaRank() {
        NodeRankByResourceQuota minQuota = NodeRankByResourceQuota.MIN_QUOTA_RANK;
        NodeRankByResourceQuota minQuota2 = NodeRankByResourceQuota.MIN_QUOTA_RANK;

        assertEquals(0, minQuota.compareTo(minQuota2));
    }

    @Test
    public void testMinQuotaRankVsNotExceededQuota() {
        NodeRankByResourceQuota quota = new NodeRankByResourceQuota(
                "a",
                new ResourceQuotaStats(ONE_HUNDRED, TEN)
        );

        SortedSet<NodeRankByResourceQuota> set = new TreeSet<>();
        set.add(quota);
        set.add(NodeRankByResourceQuota.MIN_QUOTA_RANK);

        assertEquals(quota, set.first());
        assertEquals(NodeRankByResourceQuota.MIN_QUOTA_RANK, set.last());
    }

    @Test
    public void testQuotaRankOrdering(){
        NodeRankByResourceQuota quotaA = new NodeRankByResourceQuota(
                "a",
                new ResourceQuotaStats(ONE_HUNDRED, TEN)
        );

        NodeRankByResourceQuota quotaB = new NodeRankByResourceQuota(
                "b",
                new ResourceQuotaStats(THOUSAND, TEN)
        );

        SortedSet<NodeRankByResourceQuota> set = new TreeSet<>();
        set.add(quotaA);
        set.add(quotaB);

        assertEquals(quotaB, set.first());
        assertEquals(quotaA, set.last());
    }

    @Test
    public void testPartitionAttributesOrdering(){
        NodeRankByPartitionAttributes attrA = buildAttributes(NodeNames.A, true);
        NodeRankByPartitionAttributes attrB = buildAttributes(NodeNames.B, true);
        NodeRankByPartitionAttributes attrC = buildAttributes(NodeNames.C, false);

        SortedSet<NodeRankByPartitionAttributes> set = new TreeSet<>();
        set.add(attrA);
        set.add(attrB);
        set.add(attrC);

        assertEquals(attrA, set.first());
        assertEquals(attrC, set.last());
    }

    private NodeRankByPartitionAttributes buildAttributes(String node, boolean readOnly) {
        return new NodeRankByPartitionAttributes(node, new PartitionAttributeStats(readOnly, 0, 0));
    }
}