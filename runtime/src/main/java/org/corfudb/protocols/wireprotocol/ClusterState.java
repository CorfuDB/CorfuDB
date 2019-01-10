package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

/**
 * Records the cluster state of the system.
 * This includes a map of {@link NodeState}.
 *
 * <p>Created by zlokhandwala on 11/1/18.
 */
@Data
@Builder
@AllArgsConstructor
public class ClusterState implements ICorfuPayload<ClusterState> {

    /**
     * Node's view of the cluster.
     */
    private final Map<String, NodeState> nodeStatusMap;
    private final ClusterStateNode node;

    public ClusterState(ByteBuf buf) {
        nodeStatusMap = ImmutableMap.copyOf(ICorfuPayload.mapFromBuffer(buf, String.class, NodeState.class));
        node = ClusterStateNode.CONNECTED;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, nodeStatusMap);
    }

    public int size(){
        return nodeStatusMap.size();
    }

    public enum ClusterStateNode {
        CONNECTED, DISCONNECTED
    }
}
