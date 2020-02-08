package org.corfudb.logreplication.transmitter;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

/**
 *  This class is responsible of transmitting a consistent view of the data at a given timestamp,
 *  i.e, reading and sending a snapshot of the data for the requested streams.
 *
 *  It reads log entries from the data-store through the SnapshotReader, and hands it to the
 *  SnapshotListener (the application specific callback for sending data to the remote site).
 *
 *  The SnapshotReader has a default implementation based on reads at the stream layer
 *  (no serialization/deserialization) required.
 *
 *  SnapshotListener is implemented by the application, as communication channels between sites are out of the scope
 *  of CorfuDB.
 */
@Slf4j
public class SnapshotTransmitter {

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SnapshotListener listener;
    private LogReplicationFSM fsm;
    private boolean onNext;
    private int messagesSent = 0; // Used for testing purposes

    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

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
        this.fsm = logReplicationFSM;
    }

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {
        boolean endRead = false;
        SnapshotReadMessage snapshotReadMessage = null;
        messagesSent = 0;

        // Get global tail, this will represent the timestamp for a consistent snapshot/cut of the data
        long snapshotTimestamp = runtime.getAddressSpaceView().getLogTail();

        // Skip if log is empty
        if (Address.isAddress(snapshotTimestamp)) {

            // Starting a new snapshot sync, reset the reader's snapshot timestamp
            snapshotReader.reset(snapshotTimestamp);

            while (!endRead) {
                try {
                    snapshotReadMessage = snapshotReader.read();
                    // Data Transformation / Processing
                    // Transform to byte[]

                } catch (TrimmedException te) {
                    /*
                     In the case of a Trimmed Exception, enqueue the event for state transition.

                     The event needs to carry the snapshotSyncEventId under which it was originated, so it is
                     correlated to the same initiating state.
                    */
                    fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.TRIMMED_EXCEPTION,
                            snapshotSyncEventId));

                    // Report error to the snapshotListener
                    listener.onError(LogReplicationError.TRIM_SNAPSHOT_SYNC, snapshotSyncEventId);
                } catch (Exception e) {
                    // Can Snapshot Reader send other exceptions?
                }

                if (!snapshotReadMessage.getMessages().isEmpty()) {
                    onNext = listener.onNext(snapshotReadMessage.getMessages(), snapshotSyncEventId);
                    messagesSent++;
                    observedCounter.setValue(messagesSent);
                    if (!onNext) {
                        log.error("SnapshotListener did not acknowledge next sent message(s). Notify error.");
                        // TODO (Anny): Optimize (back-off) retry on the failed send.
                        // Send on Error as snapshot sync is required.
                        listener.onError(LogReplicationError.LISTENER_ERROR, snapshotSyncEventId);
                        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CANCEL, snapshotSyncEventId));
                        break;
                    }
                }

                endRead = snapshotReadMessage.isEndRead();
            }

            log.debug("End of snapshot read found. Snapshot sync completed.");
        }

        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE, snapshotSyncEventId));
    }
}
