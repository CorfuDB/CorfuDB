package org.corfudb.logreplication.transmitter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.util.UUID;

/**
 *  This class is responsible of managing the transmission of a consistent view of the data at a given timestamp,
 *  i.e, reading and sending the full state of the data-store at a given snapshot to a remote site.
 *
 *  It reads log entries from the data-store through the SnapshotReader, and hands it to the
 *  SnapshotListener (the application specific callback for sending data to the remote site).
 *
 *  The SnapshotReader has a default implementation based on reads at the stream layer (no serialization/deserialization)
 *  required, but can be replaced by custom implementations from the application.
 *
 *  SnapshotListener is implemented by the application, as communication channels between sites are out of the scope
 *  of CorfuDB.
 */
@Slf4j
public class SnapshotTransmitter {

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SnapshotListener listener;
    private LogReplicationFSM logReplicationFSM;

    /**
     * Constructor
     *
     * @param runtime corfu runtime
     * @param snapshotReaderImpl implementation of snapshot reader
     * @param snapshotListenerImpl implementation of snapshot listener (application callback)
     * @param logReplicationFSM log replication state machine
     */
    public SnapshotTransmitter(CorfuRuntime runtime, SnapshotReader snapshotReaderImpl,
                               SnapshotListener snapshotListenerImpl, LogReplicationFSM logReplicationFSM) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReaderImpl;
        this.listener = snapshotListenerImpl;
        /*
         * The Log Replication FSM is required to enqueue internal events that cause state transition.
         */
        this.logReplicationFSM = logReplicationFSM;
    }

    /**
     * Initiate snapshot sync transmission.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
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
                  In the case of Trimmed Exception, enqueue the event for state transition.

                  The event needs to carry the snapshotSyncEventId under which it was originated, so it is
                  correlated to the same initiating state.
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
