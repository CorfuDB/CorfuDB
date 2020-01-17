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

    /**
     * Current cluster state, updated by failure detector on current iteration.
     */
    @Getter
    @NonNull
    private final AtomicReference<ClusterState> clusterView;

    /**
     * Refreshes the cluster view based on the local endpoint and the snapshot epoch at which the
     * node states are updated.
     *
     * @param layout Snapshot layout.
     */
    public void refreshClusterView(@NonNull Layout layout, @NonNull PollReport report) {
        log.trace("Refresh cluster view, layout: {}", layout);

        clusterView.set(report.getClusterState());
    }

    /**
     * Returns current cluster state.
     * @return current cluster state
     */
    public ClusterState getClusterView(){
        return clusterView.get();
    }
}
