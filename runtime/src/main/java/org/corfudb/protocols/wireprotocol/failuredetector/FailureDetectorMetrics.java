package org.corfudb.protocols.wireprotocol.failuredetector;

import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.util.JsonUtils;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Full information about failure that changed cluster state.
 * It can be saved in history and provide full information about a cluster and a failure.
 * The decision made by a node can be reproduced using this information.
 */
@Builder
@AllArgsConstructor
public class FailureDetectorMetrics implements ICorfuPayload<FailureDetectorMetrics> {
    //state
    private final String localNode;
    private final ConnectivityGraph graph;

    //action
    private final FailureDetectorAction action;
    private final NodeRank failed;
    private final NodeRank healed;

    //layout
    private final List<String> layout;
    private final List<String> unresponsiveNodes;
    private final long epoch;

    public String toJson(){
        return JsonUtils.toJson(this);
    }

    public FailureDetectorMetrics(ByteBuf buf) {
        localNode = ICorfuPayload.fromBuffer(buf, String.class);
        graph = ICorfuPayload.fromBuffer(buf, ConnectivityGraph.class);
        action = FailureDetectorAction.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        failed = ICorfuPayload.fromBuffer(buf, NodeRank.class);
        healed = ICorfuPayload.fromBuffer(buf, NodeRank.class);
        layout = ICorfuPayload.listFromBuffer(buf, String.class);
        unresponsiveNodes = ICorfuPayload.listFromBuffer(buf, String.class);
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, localNode);
        ICorfuPayload.serialize(buf, graph);
        ICorfuPayload.serialize(buf, action.name());
        ICorfuPayload.serialize(buf, failed == null ? NodeRank.EMPTY_NODE_RANK: failed);
        ICorfuPayload.serialize(buf, healed == null ? NodeRank.EMPTY_NODE_RANK: healed);
        ICorfuPayload.serialize(buf, layout);
        ICorfuPayload.serialize(buf, unresponsiveNodes);
        ICorfuPayload.serialize(buf, epoch);
    }

    public enum FailureDetectorAction {
        FAIL, HEAL, EXTERNAL_UPDATE
    }

    /**
     * Simplified version of a ClusterGraph, contains only connectivity information.
     */
    @AllArgsConstructor
    public static class ConnectivityGraph implements ICorfuPayload<ConnectivityGraph> {
        private final NavigableSet<NodeConnectivity> graph;

        public ConnectivityGraph(ByteBuf buf){
            graph = new TreeSet<>(ICorfuPayload.setFromBuffer(buf, NodeConnectivity.class));
        }

        @Override
        public void doSerialize(ByteBuf buf) {
            ICorfuPayload.serialize(buf, graph);
        }
    }
}
