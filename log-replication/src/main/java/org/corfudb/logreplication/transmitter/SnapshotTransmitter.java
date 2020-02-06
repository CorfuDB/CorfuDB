package org.corfudb.logreplication.transmitter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.UUID;

/**
 * A class that reads a snapshot and transmits it.
 */
@Slf4j
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

    public void transmit(UUID snapshotSyncEventId) {
        boolean endRead = false;
        SnapshotReadMessage snapshotReadMessage = null;

        // Get global tail, this will represent the snapshot for a consistent cut of the data
        long snapshotTimestamp = runtime.getAddressSpaceView().getLogTail();

        snapshotReader.reset(snapshotTimestamp);

        while(!endRead) {
            try {
                snapshotReadMessage = snapshotReader.read();
            } catch (TrimmedException te) {
                /*
                  In the case of Trimmed Exception enqueue event.

                  We need to bind the event to the snapshotSyncEventId so it correlates
                  to the same initiating state.
                 */
                logReplicationFSM
                        .input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.TRIMMED_EXCEPTION,
                                snapshotSyncEventId));

                // Report error to the snapshotListener
                listener.onError(LogReplicationError.TRIM_SNAPSHOT_SYNC, snapshotSyncEventId);
            }

            if (!snapshotReadMessage.getMessages().isEmpty()) {
                listener.onNext(snapshotReadMessage.getMessages(), snapshotSyncEventId);
            } else {
                log.warn("Snapshot Read Message is EMPTY. End record should be found.");
            }
            endRead = snapshotReadMessage.isEndRead();
        }

        log.debug("End of snapshot read found. Snapshot sync completed.");

        // We need to bind the event to the snapshotSyncEventId so it correlates
        // to the same initiating state.
        logReplicationFSM
                .input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                        snapshotSyncEventId));
    }
}
