package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.ToString;

/**
 * Records the cluster state of the system.
 * This includes a map of {@link NodeState}.
 *
 * <p>Created by zlokhandwala on 11/1/18.
 */
@Data
@Builder
@AllArgsConstructor
@ToString
public class ClusterState implements ICorfuPayload<ClusterState> {

    /**
     * Node's view of the cluster. The node collects states from all the other nodes in the cluster.
     * For instance, three node cluster:
     *  {"a": {"endpoint": "a", "connectivity":{"a": true, "b": true, "c": true}}}
     *  {"b": {"endpoint": "b", "connectivity":{"a": true, "b": true, "c": false}}}
     *  {"c": {"endpoint": "c", "connectivity":{"a": true, "b": false, "c": true}}}
     */
    @Singular
    private final Map<String, NodeState> nodes;

    public ClusterState(ByteBuf buf) {
        nodes = ImmutableMap.copyOf(ICorfuPayload.mapFromBuffer(buf, String.class, NodeState.class));
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, nodes);
    }

    public int size(){
        return nodes.size();
    }

    public NodeState getNode(String endpoint) {
        return nodes.get(endpoint);
    }
}
