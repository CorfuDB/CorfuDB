package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.NodeNames;
import org.corfudb.infrastructure.NodeNames.NodeName;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class NodeStateTestUtil {

    public static final String A = NodeName.a.name();
    public static final String B = NodeName.b.name();
    public static final String C = NodeName.c.name();

    private NodeStateTestUtil() {
        //prevent creating class instances
    }

    public static NodeState nodeState(String endpoint, long epoch, Optional<FileSystemStats> fsStats,
                                      ConnectionStatus... connectionStates) {
        Map<String, ConnectionStatus> connectivity = new HashMap<>();
        for (int i = 0; i < connectionStates.length; i++) {
            connectivity.put(NodeNames.NODE_NAMES.get(i).name(), connectionStates[i]);
        }

        NodeConnectivity nodeConnectivity = NodeConnectivity.builder()
                .endpoint(endpoint)
                .type(NodeConnectivityType.CONNECTED)
                .connectivity(ImmutableMap.copyOf(connectivity))
                .epoch(epoch)
                .build();

        return NodeState.builder()
                .sequencerMetrics(SequencerMetrics.READY)
                .connectivity(nodeConnectivity)
                .fileSystem(fsStats)
                .build();
    }

    public static NodeState nodeState(String endpoint, long epoch, ConnectionStatus... connectionStates) {
        return nodeState(endpoint, epoch, Optional.empty(), connectionStates);
    }
}
