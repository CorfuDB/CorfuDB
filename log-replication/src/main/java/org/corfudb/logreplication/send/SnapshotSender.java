package org.corfudb.logreplication.send;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 *  This class is responsible of transmitting a consistent view of the data at a given timestamp,
 *  i.e, reading and sending a snapshot of the data for the requested streams.
 *
 *  It reads log entries from the data-store through the SnapshotReader, and hands it to the
 *  DataSender (the application specific callback for sending data to the remote site).
 *
 *  The SnapshotReader has a default implementation based on reads at the stream layer
 *  (no serialization/deserialization) required.
 *
 *  DataSender is implemented by the application, as communication channels between sites are out of the scope
 *  of CorfuDB.
 */
@Slf4j
public class SnapshotSender {

    // TODO (probably move to a configuration file)
    public static final int SNAPSHOT_BATCH_SIZE = 5;

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private DataSender dataSender;
    private ReadProcessor readProcessor;
    private LogReplicationFSM fsm;
    private long baseSnapshotTimestamp;

    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private volatile boolean stopSnapshotSync = false;

    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM fsm) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.dataSender = dataSender;
        this.readProcessor = readProcessor;
        this.fsm = fsm;
    }

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {

        log.debug("Running snapshot sync for {} on baseSnapshot {}", snapshotSyncEventId, baseSnapshotTimestamp);

        boolean endRead = false;    // Flag indicating the snapshot sync is completed
        boolean cancel = false;     // Flag indicating snapshot sync needs to be canceled
        int messagesSent = 0;
        SnapshotReadMessage snapshotReadMessage;

        // Skip if no data is present in the log
        if (Address.isAddress(baseSnapshotTimestamp)) {

            // Read and Send Batch Size messages, unless snapshot is completed before (endRead)
            // or snapshot sync is stopped
            while (messagesSent < SNAPSHOT_BATCH_SIZE && !endRead && !stopSnapshotSync) {
                try {
                    snapshotReadMessage = snapshotReader.read();
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    log.warn("Cancel snapshot sync due to trimmed exception.", te);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.TRIM_SNAPSHOT_SYNC);
                    cancel = true;
                    break;
                } catch (Exception e) {
                    log.error("Caught exception during snapshot sync", e);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN);
                    cancel = true;
                    break;
                }

                // messages may be empty, send regardless.
                List<DataMessage> messages = snapshotReadMessage.getMessages();
                endRead = snapshotReadMessage.isEndRead();
                // Send message to dataSender (application)
                if (!dataSender.send(messages, snapshotSyncEventId, endRead)) {
                    // TODO: Optimize (back-off) retry on the failed send.
                    log.error("DataSender did not acknowledge next sent message(s). Notify error.");
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.SENDER_ERROR);
                    cancel = true;
                    break;
                }

                messagesSent++;
                observedCounter.setValue(messagesSent);
            }

            if (endRead) {
                // Snapshot Sync Completed
                log.info("Snapshot sync completed for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
                snapshotSyncComplete(snapshotSyncEventId);
            } else if (!cancel) {
                // Terminated due to number of batch messages being sent. This snapshot sync needs to
                // continue.
                log.debug("Snapshot sync continue for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);

                // Note: Snapshot Sync is not performed continuous as for the case of multi-site replication
                // the shared thread pool could be lower than the number of sites, so we assign resources in
                // a round robin fashion.
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } else {
            log.info("Snapshot sync completed for {} as there is not data in the log.", snapshotSyncEventId);
            dataSender.send(Collections.emptyList(), snapshotSyncEventId, true);
            snapshotSyncComplete(snapshotSyncEventId);
        }
    }

    private void snapshotSyncComplete(UUID snapshotSyncEventId) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp)));
    }

    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error) {
        // Enqueue cancel event
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncEventId)));

        // Report error to the application through the dataSender
        dataSender.onError(error, snapshotSyncEventId);
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
