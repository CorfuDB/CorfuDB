package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;

/**
 * Cluster State Context maintains shared context composing of cluster connectivity.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
public class ClusterStateContext {

    // Number of local heartbeat increments after which a stale heartbeat is detected as decayed.
    private static final long NODE_STATE_DECAY_CONSTANT = 3;
    // Heartbeat counter to convey the freshness of the cluster state views.
    private volatile long heartbeatCounter = 0;

    //  Local copy of the local node's sequencer server metrics.
    @Getter
    private volatile SequencerMetrics localSequencerMetrics
            = new SequencerMetrics(SequencerStatus.UNKNOWN);

    // A view of peers' connectivity.
    // Connectivity of any unresponsive nodes responding. Updated by HealingDetector.
    private volatile Set<String> responsiveNodesState = Collections.emptySet();
    // Connectivity of any responsive nodes not responding. Updated by FailureDetector.
    private volatile Set<String> unresponsiveNodesState = Collections.emptySet();
    // Cluster View compiled by aggregating latest peer views from heartbeat responses.
    private final Map<String, NodeHeartbeat> clusterView = new HashMap<>();

    /**
     * Increment local heartbeat counter.
     */
    public synchronized void incrementHeartbeat() {
        heartbeatCounter++;
    }

    /**
     * Fetches the latest snapshot of the cluster state.
     *
     * @return Returns an immutable copy of the cluster state.
     */
    public synchronized ClusterState getClusterState() {
        return new ClusterState(ImmutableMap.copyOf(clusterView.entrySet().stream()
                .filter(nodeHeartbeatSnapshotEntry -> !nodeHeartbeatSnapshotEntry.getValue().isDecayed())
                .collect(Collectors.toMap(Entry::getKey, o -> o.getValue().getNodeState()))));
    }

    /**
     * Creates a heartbeat timestamp from the node state object received.
     *
     * @param nodeState NodeState to used.
     * @return HeartbeatTimestamp.
     */
    private HeartbeatTimestamp getHeartbeatFromNodeState(@NonNull NodeState nodeState) {
        return new HeartbeatTimestamp(nodeState.getEpoch(), nodeState.getHeartbeatCounter());
    }

    /**
     * Node State is detected as stale if we have an existing entry in our decay map and the node state has a heartbeat
     * counter lesser than or equal to this decay map entry.
     *
     * @param nodeState NodeState's heartbeat to be fetched and compared.
     * @return True if the node state is stale. False otherwise.
     */
    private boolean isNodeStateStale(NodeState nodeState) {
        final NodeHeartbeat nodeHeartbeat
                = clusterView.get(NodeLocator.getLegacyEndpoint(nodeState.getEndpoint()));
        return nodeHeartbeat != null && getHeartbeatFromNodeState(nodeState)
                .compareTo(getHeartbeatFromNodeState(nodeHeartbeat.getNodeState())) <= 0;
    }

    /**
     * Updates the a particular node state in the cluster view.
     *
     * @param endpoint  Endpoint of whose node state is to be updated.
     * @param nodeState Node state to be updated.
     */
    private void updateNodeState(@NonNull String endpoint, @NonNull NodeState nodeState, @NonNull Layout layout) {
        clusterView.compute(endpoint, (s, nodeHeartbeat) ->
                nodeHeartbeat != null && isNodeStateStale(nodeState) ? nodeHeartbeat
                        : new NodeHeartbeat(nodeState,
                        new HeartbeatTimestamp(layout.getEpoch(), heartbeatCounter)));
    }

    /**
     * Updates the cluster view based on another cluster view received in the heartbeat response.
     * The latest Node states from this cluster state are retained and the stale ones are
     * discarded.
     *
     * @param clusterState Cluster state to update the local copy of the cluster state with.
     */
    public synchronized void updateNodeState(@NonNull ClusterState clusterState, @NonNull Layout currentLayout) {
        clusterState.getNodeStatusMap().forEach((endpoint, nodeState) ->
                updateNodeState(endpoint, nodeState, currentLayout));
    }

    /**
     * Update the decay flags in the NodeHeartbeats if the cluster state contains a stale NodeState.
     */
    private void markDecayedNodeStates(Layout layout, final long heartbeatCounter) {

        clusterView.forEach((endpoint, nodeHeartbeat) -> {
            HeartbeatTimestamp currentTimestamp = new HeartbeatTimestamp(layout.getEpoch(), heartbeatCounter);
            HeartbeatTimestamp lastSeen = nodeHeartbeat.getLocalHeartbeatTimestamp();
            if (currentTimestamp.epoch > lastSeen.epoch
                    || (currentTimestamp.epoch == lastSeen.epoch
                    && currentTimestamp.counter > lastSeen.counter + NODE_STATE_DECAY_CONSTANT)) {
                nodeHeartbeat.setDecayed(true);
            }
        });
    }

    /**
     * Refreshes the cluster view based on the local endpoint and the snapshot epoch at which the
     * node states are updated.
     *
     * @param localEndpoint  Local Endpoint whose node state entry is to be updated in the cluster
     *                       view map.
     * @param snapshotLayout Snapshot layout.
     */
    public synchronized void refreshClusterView(@NonNull String localEndpoint,
                                                @NonNull Layout snapshotLayout) {
        // Increment heartbeat counter.
        incrementHeartbeat();

        Map<String, Boolean> peerConnectivityDeltaMap = new HashMap<>();
        // All nodes detected as unreachable by the failure detector are added to the map.
        unresponsiveNodesState.forEach(s -> peerConnectivityDeltaMap.put(s, false));
        // All nodes which are in the unresponsive servers set in the layout excluding the ones detected as reachable
        // by the healing detector are added to the layout with flag false (no connectivity).
        snapshotLayout.getUnresponsiveServers().stream()
                // Prune out servers now responding to heartbeats.
                .filter(unresponsiveServer -> !responsiveNodesState.contains(unresponsiveServer))
                .forEach(unresponsiveServer -> peerConnectivityDeltaMap.put(unresponsiveServer, false));

        NodeState localNodeState = new NodeState(NodeLocator.parseString(localEndpoint),
                snapshotLayout.getEpoch(), heartbeatCounter, localSequencerMetrics, peerConnectivityDeltaMap);
        clusterView.put(localEndpoint, new NodeHeartbeat(localNodeState,
                new HeartbeatTimestamp(snapshotLayout.getEpoch(), heartbeatCounter)));

        // Decay any stale node states in the cluster view.
        markDecayedNodeStates(snapshotLayout, heartbeatCounter);

        // Remove node endpoints which are no longer a part of the layout from the clusterView.
        clusterView.keySet().retainAll(snapshotLayout.getAllServers());
    }

    /**
     * Print the clusterView for debugging.
     */
    public synchronized void logClusterView() {
        clusterView.forEach((endpoint, nodeHeartbeat) -> log.info("{} => {}", endpoint, nodeHeartbeat.toString()));
    }

    /**
     * Updates the set of responsive nodes received from the healing detector.
     *
     * @param responsiveNodesState Set of responsive nodes.
     */
    public synchronized void updateResponsiveNodes(@NonNull Set<String> responsiveNodesState) {
        this.responsiveNodesState = responsiveNodesState;
    }

    /**
     * Updates the set of unresponsive nodes received from the failure detector.
     *
     * @param unresponsiveNodesState Set of responsive nodes.
     */
    public synchronized void updateUnresponsiveNodes(@NonNull Set<String> unresponsiveNodesState) {
        this.unresponsiveNodesState = unresponsiveNodesState;
    }

    /**
     * Updates the local server metrics.
     *
     * @param localSequencerMetrics local server metrics to be updated.
     */
    public synchronized void updateLocalServerMetrics(@NonNull SequencerMetrics localSequencerMetrics) {
        this.localSequencerMetrics = localSequencerMetrics;
    }
}
