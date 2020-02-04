package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.LogReplicationContext;

/**
 * A wrapper class for Snapshot Reader
 */
public class SnapshotReaderWrapper {

    private SnapshotReader snapshotReader;
    private LogReplicationContext context;

    public SnapshotReaderWrapper(SnapshotReader snapshotReaderImpl, LogReplicationContext context) {
        this.snapshotReader = snapshotReaderImpl;
        this.context = context;
    }

    public LogReplicationContext sync() {
        snapshotReader.sync();
        return context;
    }
}
