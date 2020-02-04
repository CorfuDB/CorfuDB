package org.corfudb.logreplication.receiver;

import org.corfudb.runtime.CorfuRuntime;

import java.util.List;

public class ReplicationRxManager {
    private CorfuRuntime runtime;

    public ReplicationRxManager(CorfuRuntime rt) {
        runtime = rt;
    }

    public void apply(RxMessage message) {
        // Buffer data (out of order) and apply
    }

    public void apply(List<RxMessage> message) {
        // Buffer data (out of order) and apply
    }
}
