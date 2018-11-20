package org.corfudb.infrastructure.management;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.corfudb.protocols.wireprotocol.NodeState;

/**
 * NodeHeartbeat is a wrapper over the NodeState object received from a remote node.
 * This object contains the local timestamp at which this remote node state was received.
 * This local timestamp is then used to decide if this NodeHeartbeat is too stale to be marked as decayed.
 */
@Data
@RequiredArgsConstructor
public class NodeHeartbeat {

    /**
     * Node state received from remote node.
     */
    private final NodeState nodeState;

    /**
     * Local heartbeat timestamp at which this node state was recorded.
     */
    private final HeartbeatTimestamp localHeartbeatTimestamp;

    /**
     * Flag denoting whether this node heartbeat is decayed or can be taken into consideration while creating the
     * cluster state.
     */
    private boolean decayed = false;
}
