package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;

public class DataReceiver {
    private LogReplicationContext context;
    private CorfuRuntime runtime;

    public DataReceiver(CorfuRuntime rt) {
        runtime = rt;
    }

    public void apply(RxMessage message) {
        // Buffer data (out of order) and apply
    }


}
