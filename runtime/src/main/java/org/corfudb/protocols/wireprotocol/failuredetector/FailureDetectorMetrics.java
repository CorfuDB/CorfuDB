package org.corfudb.protocols.wireprotocol.failuredetector;

import java.util.List;
import java.util.NavigableSet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.corfudb.util.JsonUtils;

/**
 * Full information about failure that changed cluster state.
 * It can be saved in history and provide full information about a cluster and a failure.
 * The decision made by a node can be reproduced using this information.
 */
@Builder
@AllArgsConstructor
public class FailureDetectorMetrics {
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

    public enum FailureDetectorAction {
        FAIL, HEAL, EXTERNAL_UPDATE
    }

    /**
     * Simplified version of a ClusterGraph, contains only connectivity information.
     */
    @AllArgsConstructor
    public static class ConnectivityGraph {
        private final NavigableSet<NodeConnectivity> graph;
    }
}
