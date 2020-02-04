package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;

public class DataReceiver {
    private LogReplicationContext context;
    private CorfuRuntime runtime;

    public DataReceiver(CorfuRuntime rt) {
        runtime = rt;
    }

    public void apply(RxMessage message) {
        // Buffer data (out of order) and apply
    }

    public void apply(List<RxMessage> message) {
        // Buffer data (out of order) and apply
    }
}
