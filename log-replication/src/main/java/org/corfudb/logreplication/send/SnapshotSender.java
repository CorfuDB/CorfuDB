package org.corfudb.logreplication.send;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.send.logreader.ReadProcessor;
import org.corfudb.logreplication.send.logreader.SnapshotReadMessage;
import org.corfudb.logreplication.send.logreader.SnapshotReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

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
    public static final int DEFAULT_TIMEOUT = 5000;

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SenderBufferManager dataSenderBufferManager;
    private LogReplicationFSM fsm;
    private long baseSnapshotTimestamp;

    // This flag will indicate the start of a snapshot sync, so start snapshot marker is sent once.
    private boolean startSnapshotSync = true;

    // Flag indicating the snapshot sync reader has read all entries
    boolean readingCompleted;


    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private volatile boolean stopSnapshotSync = false;

    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, LogReplicationFSM fsm) {
        this.runtime = runtime;;
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        readingCompleted = false;
        dataSenderBufferManager = new SnapshotSenderBufferManager(dataSender);
    }


    /**
     * Snapshot sync phase I: read data and send messages through the buffer.
     * @param snapshotSyncEventId
     */
    public void readAndSendMessage(UUID snapshotSyncEventId) {
        boolean cancel = false;     // Flag indicating snapshot sync needs to be canceled
        int messagesSent = 0;       // Limit the number of messages to SNAPSHOT_BATCH_SIZE. The reason we need to limit
        // is because by design several state machines can share the same thread pool,
        // therefore, we need to hand the thread for other workers to execute.

        SnapshotReadMessage snapshotReadMessage;

        // Skip if no data is present in the log
        if (Address.isAddress(baseSnapshotTimestamp)) {
            // Read and Send Batch Size messages, unless snapshot is completed before (endRead)
            // or snapshot sync is stopped
            dataSenderBufferManager.resend();
            while (messagesSent < SNAPSHOT_BATCH_SIZE && !dataSenderBufferManager.getPendingMessages().isFull() &&!readingCompleted && !stopSnapshotSync) {

                try {
                    snapshotReadMessage = snapshotReader.read(snapshotSyncEventId);
                    readingCompleted = snapshotReadMessage.isEndRead();
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    log.warn("Cancel snapshot sync due to trimmed exception.", te);
                    dataSenderBufferManager.reset(Address.NON_ADDRESS);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.TRIM_SNAPSHOT_SYNC);
                    cancel = true;
                    break;
                } catch (Exception e) {
                    log.error("Caught exception during snapshot sync", e);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN);
                    cancel = true;
                    break;
                }

                messagesSent += processReads(snapshotReadMessage.getMessages(), snapshotSyncEventId, readingCompleted);
                observedCounter.setValue(messagesSent);
            }

            if (readingCompleted) {
                readingCompleteResendMessagesInBuffer(snapshotSyncEventId);
            }

            if(!cancel) {
                // Maximum number of batch messages sent. This snapshot sync needs to continue.

                // Snapshot Sync is not performed in a single run, as for the case of multi-site replication
                // the shared thread pool could be lower than the number of sites, so we assign resources in
                // a round robin fashion.
                log.trace("Snapshot sync continue for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } else {
            log.info("Snapshot sync completed for {} as there is no data in the log.", snapshotSyncEventId);

            // Generate a special LogReplicationEntry with only metadata (used as start marker on receiver side
            // to complete snapshot sync and send the right ACK)
            try {
                dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
                dataSenderBufferManager.sendWithBuffering(getSnapshotSyncEndMarker(snapshotSyncEventId));
                //snapshotSyncAck.get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
                snapshotSyncComplete(snapshotSyncEventId);
                //System.out.print("\nSnapshot Sender Snapshot End Ack recieved " + snapshotSyncAck);
            } catch (Exception e) {
                //todo: generate an event for discovery service
                log.warn("While sending data, caught an exception. Will notify discovery service");
            }
        }
    }

    /**
     * The reader has finished reading all the entries and message has been delivered.
     * It is responsible to resend messages in the buffer and wait till the receiver send an ACK
     * for the snapshot end marker message.
     * @param snapshotSyncEventId
     */
    public void readingCompleteResendMessagesInBuffer(UUID snapshotSyncEventId) {
        LogReplicationEntry ack = dataSenderBufferManager.resend();
        System.out.print("\nreadingCompleteResendMessagesInBuffer ack " + ack.getMetadata());

        // Block until ACK from last sent message is received]
        try {
            if (ack != null && ack.getMetadata().getSnapshotTimestamp() == baseSnapshotTimestamp &&
                    ack.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END) {
                // Snapshot Sync Completed
                log.info("Snapshot sync completed for {} on timestamp {}, ack{}", snapshotSyncEventId,
                        baseSnapshotTimestamp, ack.getMetadata());
                System.out.print("\nSnapshot sync completed for {} on timestamp {}, ack{}" + snapshotSyncEventId
                        + " ack " + ack.getMetadata());

                snapshotSyncComplete(snapshotSyncEventId);
            } else if (ack != null && ack.getMetadata().getSnapshotTimestamp() != baseSnapshotTimestamp){
                log.warn("Expected ack for {}, but received for a different snapshot {}", baseSnapshotTimestamp,
                        ack.getMetadata());
                throw new Exception("Wrong base snapshot ack");
            } else {
                // Maximum number of batch messages sent. This snapshot sync needs to continue.

                // Snapshot Sync is not performed in a single run, as for the case of multi-site replication
                // the shared thread pool could be lower than the number of sites, so we assign resources in
                // a round robin fashion.
                log.trace("Snapshot sync continue for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } catch (Exception e) {
            log.error("Exception caught while blocking on snapshot sync {}, ack for {}",
                    snapshotSyncEventId, baseSnapshotTimestamp, e);
            snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN);
        }
    }


    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     * There are two phases:
     * Phase I: reading data and send message through buffer till read all the data
     * Phase II: resend the data in the buffer to receiver till the SNAPSHOT_END is acknowledged
     * by the receiver. It may keep sending SNAPSHOT_END message to ping the receiver to know
     * if the connection is still up.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {

        log.info("Running snapshot sync for {} on baseSnapshot {}", snapshotSyncEventId,
                baseSnapshotTimestamp);

        if(!readingCompleted) {
            readAndSendMessage(snapshotSyncEventId);
        } else {
            readingCompleteResendMessagesInBuffer(snapshotSyncEventId);
            return;
        }
    }


    private int processReads(List<LogReplicationEntry> logReplicationEntries, UUID snapshotSyncEventId, boolean completed) {
        int numMessages = 0;

        // If we are starting a snapshot sync, send a start marker.
        if (startSnapshotSync) {
            dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
            startSnapshotSync = false;
            numMessages++;
        }

        dataSenderBufferManager.sendWithBuffering(logReplicationEntries);

        // If Snapshot is complete, add end marker
        if (completed) {
            LogReplicationEntry endDataMessage = getSnapshotSyncEndMarker(snapshotSyncEventId);
            log.info("SnapshotSender sent out SNAPSHOT_END message {} " + endDataMessage.getMetadata());
            dataSenderBufferManager.sendWithBuffering(endDataMessage);
            numMessages++;
        }

        return numMessages + logReplicationEntries.size();
    }


    /**
     * Prepare a Snapshot Sync Replication start marker.
     *
     * @param snapshotSyncEventId snapshot sync event identifier
     * @return snapshot sync start marker as LogReplicationEntry
     */
    private LogReplicationEntry getSnapshotSyncStartMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_START, fsm.getSiteConfigID(),
                snapshotSyncEventId, Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata, new byte[0]);
        return emptyEntry;
    }

    private LogReplicationEntry getSnapshotSyncEndMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_END, fsm.getSiteConfigID(), snapshotSyncEventId,
                Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata, new byte[0]);
        return emptyEntry;
    }

    /**
     * Complete Snapshot Sync, insert completion event in the FSM queue.
     *
     * @param snapshotSyncEventId unique identifier for the completed snapshot sync.
     */
    private void snapshotSyncComplete(UUID snapshotSyncEventId) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp)));
    }

    /**
     * Cancel Snapshot Sync due to an error.
     *
     * @param snapshotSyncEventId unique identifier for the snapshot sync task
     * @param error specific error cause
     */
    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error) {
        // Report error to the application through the dataSender
        dataSenderBufferManager.onError(error);

        // Enqueue cancel event, this will cause a transition to the require snapshot sync request, which
        // will notify application through the data control about this request.
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncEventId)));
    }

    /**
     * Reset due to the start of a new snapshot sync.
     */
    public void reset() {
        // TODO: Do we need to persist the lastTransferDone in the event of failover?
        // Get global tail, this will represent the timestamp for a consistent snapshot/cut of the data
        baseSnapshotTimestamp = runtime.getAddressSpaceView().getLogTail();

        // Starting a new snapshot sync, reset the logreader's snapshot timestamp
        snapshotReader.reset(baseSnapshotTimestamp);
        dataSenderBufferManager.reset(Address.NON_ADDRESS);

        stopSnapshotSync = false;
        startSnapshotSync = true;
        readingCompleted = false;
    }

    /**
     * Stop Snapshot Sync
     */
    public void stop() {
        stopSnapshotSync = true;
    }
}
