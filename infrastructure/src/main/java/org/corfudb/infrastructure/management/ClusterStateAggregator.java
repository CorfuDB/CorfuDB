package org.corfudb.infrastructure.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.NonNull;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates cluster state from a list of collected states.
 */
@Builder
public class ClusterStateAggregator {

    @NonNull
    private final String localEndpoint;

    @NonNull
    private final List<ClusterState> clusterStates;

    @NonNull
    private final ImmutableList<String> unresponsiveNodes;

    /**
     * Aggregates state from a list of poll reports.
     * In order to prevent accidentally adding a node to the list of unresponsive nodes we collect
     * PollReport more than one time (3 times by default) and then aggregate reports:
     * - if at least one report contains NodeState with CONNECTED status
     * we believe that the node is connected.
     * - use latest possible (connected) NodeState, if there is no 'connected' state then
     * any other latest state, (see {@link NodeConnectivityType}), for instance:
     * 1. reports = [CONNECTED, UNAVAILABLE, UNAVAILABLE], reports[0] will be used
     * 2. reports = [CONNECTED, UNAVAILABLE, CONNECTED], reports[2] will be used
     * 3. reports = [UNAVAILABLE, CONNECTED, UNAVAILABLE], reports[1] will be used
     * 4. reports = [CONNECTED, CONNECTED, CONNECTED], reports[2] will be used
     * 5. reports = [UNAVAILABLE, UNAVAILABLE, NOT_READY], reports[2] will be used
     *
     * @return aggregated cluster state
     */
    public ClusterState getAggregatedState() {
        if (clusterStates.isEmpty()) {
            throw new IllegalStateException("Insufficient cluster state");
        }

        if (clusterStates.size() == 1) {
            return clusterStates.get(0);
        }

        Map<String, NodeState> stateMap = clusterStates.stream()
                .map(ClusterState::getNodes)
                .reduce((prevNodes, currNodes) -> {
                    Map<String, NodeState> actualState = new HashMap<>();

                    for (String endpoint : currNodes.keySet()) {
                        NodeState prevNodeState = prevNodes.get(endpoint);
                        NodeState currNodeState = currNodes.get(endpoint);

                        //update the old node state by the new one

                        if (!prevNodeState.isConnected() && currNodeState.isConnected()) {
                            actualState.put(endpoint, currNodeState);
                        } else if (prevNodeState.isConnected() && currNodeState.isConnected()) {
                            Map<String, NodeConnectivity.ConnectionStatus> mergedStates = new HashMap<>(
                                    prevNodeState
                                            .getConnectivity()
                                            .getConnectivity());

                            currNodeState.getConnectivity().getConnectivity().forEach((k, v) ->
                                    mergedStates.merge(k, v, (item1, item2) ->
                                            item1 == NodeConnectivity.ConnectionStatus.OK
                                                    || item2 == NodeConnectivity.ConnectionStatus.OK ?
                                                    NodeConnectivity.ConnectionStatus.OK :
                                                    NodeConnectivity.ConnectionStatus.FAILED));

                            long prevNodeEpoch = prevNodeState.getConnectivity().getEpoch();
                            long curNodeEpoch = currNodeState.getConnectivity().getEpoch();
                            Preconditions.checkArgument(prevNodeEpoch == curNodeEpoch,
                                    "%s should be equal to %s", prevNodeEpoch, curNodeEpoch);
                            NodeConnectivity nodeConnectivity = NodeConnectivity.builder()
                                    .endpoint(endpoint)
                                    .type(currNodeState.getConnectivity().getType())
                                    .connectivity(ImmutableMap.copyOf(mergedStates))
                                    .epoch(currNodeState.getConnectivity().getEpoch())
                                    .build();

                            NodeState mergedNodeState = NodeState.builder()
                                    .sequencerMetrics(currNodeState.getSequencerMetrics())
                                    .connectivity(nodeConnectivity)
                                    .fileSystem(currNodeState.getFileSystem())
                                    .build();

                            actualState.put(endpoint, mergedNodeState);

                        } else if (prevNodeState.isConnected() && !currNodeState.isConnected()) {
                            actualState.put(endpoint, prevNodeState);
                        } else {
                            actualState.put(endpoint, currNodeState);
                        }
                    }

                    return ImmutableMap.copyOf(actualState);
                }).orElse(ImmutableMap.of());

        return ClusterState.builder()
                .localEndpoint(localEndpoint)
                .unresponsiveNodes(unresponsiveNodes)
                .nodes(stateMap)
                .build();
    }
}
