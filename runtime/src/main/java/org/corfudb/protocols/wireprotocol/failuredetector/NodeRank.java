package org.corfudb.protocols.wireprotocol.failuredetector;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;

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
public class NodeRank implements Comparable<NodeRank> {
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

    public NodeRank(ByteBuf buf) {
        endpoint = CorfuProtocolCommon.fromBuffer(buf, String.class);
        numConnections = CorfuProtocolCommon.fromBuffer(buf, Integer.class);
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    @ToString
    public static class NodeRankByResourceQuota implements Comparable<NodeRankByResourceQuota> {
        public static final NodeRankByResourceQuota MIN_QUOTA_RANK = new NodeRankByResourceQuota(
                "--",
                new ResourceQuotaStats(0, Long.MAX_VALUE)
        );

        private final String endpoint;
        private final ResourceQuotaStats quota;

        @Override
        public int compareTo(NodeRankByResourceQuota other) {
            //Descending order
            int availableSpaceRank = Long.compare(other.quota.available(), quota.available());
            if (availableSpaceRank != 0) {
                return availableSpaceRank;
            }

            //Ascending order
            return endpoint.compareTo(other.endpoint);
        }
    }
}
