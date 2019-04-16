package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeStateTestUtil {
    private static final List<String> NODE_NAMES = Arrays.asList(
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"
    );

    private NodeStateTestUtil() {
        //prevent creating class instances
    }

    public static NodeState nodeState(String endpoint, ConnectionStatus... connectionStates) {
        Map<String, ConnectionStatus> connectivity = new HashMap<>();
        for (int i = 0; i < connectionStates.length; i++) {
            connectivity.put(NODE_NAMES.get(i), connectionStates[i]);
        }

        NodeConnectivity nodeConnectivity = NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.CONNECTED)
                .connectivity(ImmutableMap.copyOf(connectivity))
                .build();

        return NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(nodeConnectivity)
                .build();
    }
}
