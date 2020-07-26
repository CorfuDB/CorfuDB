package org.corfudb.infrastructure.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState.INIT;
import static org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState.RESET_LOG_UNIT;
import static org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState.START;
import static org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState.STOP;
import static org.corfudb.infrastructure.server.CorfuServerStateGraph.CorfuServerState.STOP_AND_CLEAN;

public class CorfuServerStateGraph {
    private final ImmutableMap<CorfuServerState, ImmutableSet<CorfuServerState>> stateGraph;

    public CorfuServerStateGraph() {
        stateGraph = ImmutableMap
                .<CorfuServerState, ImmutableSet<CorfuServerState>>builder()
                .put(INIT, ImmutableSet.of(START))
                .put(START, ImmutableSet.of(RESET_LOG_UNIT, STOP, STOP_AND_CLEAN))
                .put(STOP, ImmutableSet.of(START))
                .put(STOP_AND_CLEAN, ImmutableSet.of(START))
                .put(RESET_LOG_UNIT, ImmutableSet.of(RESET_LOG_UNIT, STOP, STOP_AND_CLEAN))
                .build();
    }

    public boolean isTransitionLegal(CorfuServerState from, CorfuServerState to) {
        return stateGraph.get(from).contains(to);
    }

    public enum CorfuServerState {
        INIT, START, RESET_LOG_UNIT, STOP, STOP_AND_CLEAN
    }
}
