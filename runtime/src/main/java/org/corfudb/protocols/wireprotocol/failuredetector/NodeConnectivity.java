package org.corfudb.protocols.wireprotocol.failuredetector;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

@Builder
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class NodeConnectivity implements ICorfuPayload<NodeConnectivity>, Comparable<NodeConnectivity> {
    @Getter
    private final String endpoint;
    @Getter
    private final NodeConnectivityType type;
    @Getter
    @NonNull
    private final ImmutableMap<String, Boolean> connectivity;

    public NodeConnectivity(ByteBuf buf) {
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
        type = NodeConnectivityType.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        connectivity = ImmutableMap.copyOf(ICorfuPayload.mapFromBuffer(buf, String.class, Boolean.class));
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
        ICorfuPayload.serialize(buf, type.name());
        ICorfuPayload.serialize(buf, connectivity);
    }

    /**
     * Returns node status: connected, disconnected
     *
     * @param node node name
     * @return node status
     */
    public boolean getConnectionStatus(String node) {
        if (type == NodeConnectivityType.UNAVAILABLE){
            return false;
        }

        if (!connectivity.containsKey(node)){
            return false;
        }

        return connectivity.get(node);
    }

    /**
     * Get number of connected nodes
     * @return number of connected nodes
     */
    public int getConnected() {
        return connectivity.keySet().stream().mapToInt(node -> connectivity.get(node) ? 1 : 0).sum();
    }

    @Override
    public int compareTo(NodeConnectivity other) {
        return endpoint.compareTo(other.endpoint);
    }

    public enum NodeConnectivityType {
        /**
         * Two nodes are connected
         */
        CONNECTED,
        /**
         * We are unable to get node state from the node (link failure between the nodes)
         */
        UNAVAILABLE
    }
}
