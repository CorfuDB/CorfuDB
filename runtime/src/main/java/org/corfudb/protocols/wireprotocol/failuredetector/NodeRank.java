package org.corfudb.protocols.wireprotocol.failuredetector;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;

/**
 * The rank of the node in a graph. Sorting nodes according to rank used to search for
 * decision makers, failed nodes and so on. Sorting according to:
 * - Descending order of number of connections
 * - Ascending order of a node name (alphabetically)
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
public class NodeRank implements Comparable<NodeRank>, NodeRanking {
    public static final NodeRank EMPTY_NODE_RANK = new NodeRank("--", Integer.MIN_VALUE);

    private final String endpoint;
    /**
     * Number of successfully connected nodes to this node.
     */
    private final int numConnections;

    /**
     * Comparator, sorting nodes according to their number of connections and alphabetically
     *
     * @param other another node
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(NodeRank other) {
        //Descending order
        int connectionRank = Integer.compare(other.numConnections, numConnections);
        if (connectionRank != 0) {
            return connectionRank;
        }

        //Ascending order
        return endpoint.compareTo(other.endpoint);
    }

    public boolean is(String endpoint) {
        return this.endpoint.equals(endpoint);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class NodeRankByPartitionAttributes implements Comparable<NodeRankByPartitionAttributes>, NodeRanking {
        private final String endpoint;
        private final PartitionAttributeStats attr;

        @Override
        public int compareTo(NodeRankByPartitionAttributes other) {
            //Descending order
            int readOnly = Boolean.compare(other.attr.isReadOnly(), attr.isReadOnly());
            if (readOnly != 0) {
                return readOnly;
            }

            //Ascending order
            return endpoint.compareTo(other.endpoint);
        }
    }
}
