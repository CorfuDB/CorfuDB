package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.InSnapshotSyncState;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;

/**
 * A class that reads a snapshot and transmits it.
 */
public class SnapshotTransmitter {

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SnapshotListener listener;
    private LogReplicationFSM logReplicationFSM;

    public SnapshotTransmitter(CorfuRuntime runtime, SnapshotReader snapshotReaderImpl,
                               SnapshotListener snapshotListenerImpl, LogReplicationFSM logReplicationFSM) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReaderImpl;
        this.listener = snapshotListenerImpl;
        this.logReplicationFSM = logReplicationFSM;
    }

    public void transmit() {
        boolean endRead = false;
        SnapshotReadMessage snapshotReadMessage;

        // Get global tail, this will represent the snapshot for a consistent cut of the data
        long snapshotTimestamp = runtime.getAddressSpaceView().getLogTail();

        snapshotReader.reset(snapshotTimestamp);

        while(!endRead) {
            snapshotReadMessage = snapshotReader.read();
            if (!snapshotReadMessage.getMessages().isEmpty()) {
                listener.onNext(snapshotReadMessage.getMessages());
            } else {
                // Log Error
            }
            endRead = snapshotReadMessage.isEndRead();
        }

        // Insert SNAPSHOT_SYNC_COMPLETE event in the FSM queue
        logReplicationFSM
                .input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE));
    }
}
