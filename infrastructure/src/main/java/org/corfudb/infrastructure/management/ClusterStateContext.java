package org.corfudb.infrastructure.management;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster State Context maintains shared context composing of cluster connectivity.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
@Builder
public class ClusterStateContext {

    // Number of local heartbeat increments after which a stale heartbeat is detected as decayed.
    private static final long NODE_STATE_DECAY_CONSTANT = 3;

    @Getter
    @NonNull
    private final AtomicReference<ClusterState> clusterView;
    @NonNull
    private final HeartbeatCounter counter;

    /**
     * Update the decay flags in the NodeHeartbeats if the cluster state contains a stale NodeState.
     */
    private void markDecayedNodeStates(Layout layout) {
        log.debug("markDecayedNodeStates. Do nothing");
    }

    /**
     * Refreshes the cluster view based on the local endpoint and the snapshot epoch at which the
     * node states are updated.
     *
     * @param layout Snapshot layout.
     */
    public void refreshClusterView(@NonNull Layout layout, @NonNull PollReport report) {
        log.trace("Refresh cluster view, layout: {}", layout);

        clusterView.set(report.getClusterState());

        markDecayedNodeStates(layout);
    }

    public ClusterState getClusterView(){
        return clusterView.get();
    }

    public static class HeartbeatCounter {
        // Heartbeat counter to convey the freshness of the cluster state views.
        private final AtomicLong counter = new AtomicLong();

        /**
         * Increment local heartbeat counter.
         */
        public long incrementHeartbeat() {
            return counter.incrementAndGet();
        }
    }
}
