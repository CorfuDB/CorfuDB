package org.corfudb.logreplication.transmit;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    // TODO (probably move to a configuration file)
    private static final int SNAPSHOT_BATCH_SIZE = 5;

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SnapshotListener snapshotListener;
    private ReadProcessor readProcessor;
    private LogReplicationFSM fsm;
    private boolean onNext;
    private int messagesSent = 0; // Used for testing purposes
    private long baseSnapshotTimestamp;


    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private volatile boolean stopSnapshotSync = false;

    public SnapshotTransmitter(CorfuRuntime runtime, SnapshotReader snapshotReader, SnapshotListener snapshotListener,
                               ReadProcessor readProcessor,LogReplicationFSM fsm) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.snapshotListener = snapshotListener;
        this.readProcessor = readProcessor;
        this.fsm = fsm;
    }

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {
        boolean endRead = false;
        SnapshotReadMessage snapshotReadMessage = null;

        // Skip if no data is present in the log
        if (Address.isAddress(baseSnapshotTimestamp)) {

            // Do until SNAPSHOT_BATCH_SIZE messages are sent or the reader reaches the end of the reads
            while (messagesSent < SNAPSHOT_BATCH_SIZE && !endRead && !stopSnapshotSync) {
                try {
                    snapshotReadMessage = snapshotReader.read();
                    System.out.println("read");
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    /*
                     In the case of a Trimmed Exception, enqueue the event for state transition.

                     The event needs to carry the snapshotSyncEventId under which it was originated, so it is
                     correlated to the same initiating state.
                    */
                    fsm.input(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.TRIMMED_EXCEPTION,
                            new LogReplicationEventMetadata(snapshotSyncEventId)));

                    // Report error to the snapshotListener
                    snapshotListener.onError(LogReplicationError.TRIM_SNAPSHOT_SYNC, snapshotSyncEventId);
                } catch (Exception e) {
                    // Can Snapshot Reader send other exceptions?
                }

                if (!snapshotReadMessage.getMessages().isEmpty()) {
                    onNext = snapshotListener.onNext(snapshotReadMessage.getMessages(), snapshotSyncEventId);
                    messagesSent++;
                    System.out.println("sent: " + messagesSent);
                    observedCounter.setValue(messagesSent);
                    if (!onNext) {
                        log.error("SnapshotListener did not acknowledge next sent message(s). Notify error.");
                        // TODO (Anny): Optimize (back-off) retry on the failed send.
                        // Send on Error as snapshot sync is required.
                        snapshotListener.onError(LogReplicationError.LISTENER_ERROR, snapshotSyncEventId);
                        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CANCEL,
                                new LogReplicationEventMetadata(snapshotSyncEventId)));
                        break;
                    }
                } else {
                    System.out.println("Nothing!!!");
                }

                endRead = snapshotReadMessage.isEndRead();
            }

            if (endRead) {
                // Terminated due to end of reads for this snapshot sync.
                log.debug("End of snapshot read found. Snapshot sync completed for {} on timestamp {}",
                        snapshotSyncEventId, baseSnapshotTimestamp);
                snapshotSyncComplete(snapshotSyncEventId);
            } else {
                // Terminated due to number of batch messages being sent. This snapshot sync needs to
                // continue.

                // Note: Snapshot Sync is not performed continuous as for the case of multi-site replication
                // the shared thread pool could be lower than the number of sites, so we assign resources in
                // a round robin fashion.
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } else {
            snapshotSyncComplete(snapshotSyncEventId);
        }
    }

    private void snapshotSyncComplete(UUID snapshotSyncEventId) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        snapshotListener.complete(snapshotSyncEventId);

        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp)));
    }

    /**
     * Reset due to the start of a new snapshot sync.
     */
    public void reset() {
        // TODO: Do we need to persist the baseSnapshotTimestamp in the event of failover?

        // Get global tail, this will represent the timestamp for a consistent snapshot/cut of the data
        baseSnapshotTimestamp = runtime.getAddressSpaceView().getLogTail();

        // Starting a new snapshot sync, reset the reader's snapshot timestamp
        snapshotReader.reset(baseSnapshotTimestamp);

        messagesSent = 0;
        stopSnapshotSync = false;
    }

    /**
     * Stop Snapshot Sync
     */
    public void stop() {
        stopSnapshotSync = true;
    }

    /**
     * Retrieve base snapshot timestamp.
     *
     * @return base snapshot timestamp
     */
    public long getBaseSnapshotTimestamp() {
        return this.baseSnapshotTimestamp;
    }
}
