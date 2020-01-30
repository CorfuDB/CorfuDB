package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.LogReplicationContext;


public class SnapshotReader {

    LogReplicationContext context;

    public SnapshotReader(LogReplicationContext context) {
        this.context = context;
    }

    public void sync() {
    }
}
