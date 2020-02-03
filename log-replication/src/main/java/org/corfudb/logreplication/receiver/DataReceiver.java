package org.corfudb.logreplication.receiver;

import org.corfudb.runtime.CorfuRuntime;

public class DataReceiver {

    private CorfuRuntime runtime;

    public DataReceiver(CorfuRuntime rt) {
        runtime = rt;
    }

    public void apply(RxMessage message) {
        // Buffer data (out of order) and apply
    }

}
