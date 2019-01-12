package org.corfudb.infrastructure.management;

import static org.junit.Assert.assertEquals;

import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.junit.Test;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

public class NodeRankTest {

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

}