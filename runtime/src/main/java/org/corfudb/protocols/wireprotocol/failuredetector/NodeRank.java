package org.corfudb.protocols.wireprotocol.failuredetector;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
public class NodeRank implements ICorfuPayload<NodeRank>,  Comparable<NodeRank> {
    public static final NodeRank EMPTY_NODE_RANK = new NodeRank("--", Integer.MIN_VALUE);

    private final String endpoint;
    private final int numConnections;

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

    public NodeRank(ByteBuf buf){
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
        numConnections = ICorfuPayload.fromBuffer(buf, Integer.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
        ICorfuPayload.serialize(buf, numConnections);
    }
}
