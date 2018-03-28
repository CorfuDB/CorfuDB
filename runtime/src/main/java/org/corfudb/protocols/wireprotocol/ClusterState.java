package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
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

    public ClusterState(ByteBuf buf) {
        nodeStatusMap = ICorfuPayload.mapFromBuffer(buf, String.class, NodeState.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, nodeStatusMap);
    }
}
