package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.UUID;

import static java.lang.Thread.sleep;

/**
 *  This class is responsible of transmitting a consistent view of the data at a given timestamp,
 *  i.e, reading and sending a snapshot of the data for the requested streams.
 *
 *  It reads log entries from the data-store through the SnapshotReader, and hands it to the
 *  DataSender (the application specific callback for sending data to the remote cluster) through BufferManager.
 *
 *  The SnapshotReader has a default implementation based on reads at the stream layer
 *  (no serialization/deserialization) required.
 *
 *  DataSender is implemented by the application, as communication channels between sites are out of the scope
 *  of CorfuDB.
 */
@Slf4j
public class SnapshotSender {

    public static int DEFAULT_SNAPSHOT_BATCH_SIZE = 100;
    public static final int DEFAULT_TIMEOUT = 5000;

    // While waiting for the standby site applying the snapshot,
    // the interval it polls the standby site.
    static final int SLEEP_INTERVAL = 1000;

    private CorfuRuntime runtime;

    // Responsible reading log data from corfu
    private SnapshotReader snapshotReader;

    // Responsible buffering outgoing data that hasn't been ACKed yet.
    private SenderBufferManager dataSenderBufferManager;

    private LogReplicationFSM fsm;

    // Snapshot timestamp.
    private long baseSnapshotTimestamp;
    private final int snapshotSyncBatchSize;
    boolean completed = false;    // Flag indicating the snapshot reading phase is completed

    // Flag indicating the full snapshot sync's phase I, data reading and transferring phase, completed.
    boolean readingCompleted = false;

    // This flag will indicate the starting of a snapshot sync, so start snapshot marker is sent once.
    private boolean startSnapshotSync = true;


    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    // Enforce a stop of sending message and snapshot sync process.
    private volatile boolean stopSnapshotSync = false;

    // Used to directly query the receiver's status.
    private DataSender dataSender;

    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, int snapshotSyncBatchSize, LogReplicationFSM fsm) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        this.snapshotSyncBatchSize = snapshotSyncBatchSize <= 0 ? DEFAULT_SNAPSHOT_BATCH_SIZE : snapshotSyncBatchSize;
        this.dataSenderBufferManager = new SnapshotSenderBufferManager(dataSender);
        this.dataSender = dataSender;
    }


    /**
     * Read the data from corfu table and pass the data to the sender through buffer manager.
     * @param snapshotSyncEventId
     */
    public void readAndTransmit(UUID snapshotSyncEventId) {
        int messagesSent = 0;
        SnapshotReadMessage snapshotReadMessage;
        boolean cancel = false;

        // For each cycle, read at most SNAPSHOT_BATCH_SIZE of messages or untill the buffer is full.
        while (messagesSent < snapshotSyncBatchSize && !dataSenderBufferManager.getPendingMessages().isFull()
                &&!readingCompleted && !stopSnapshotSync) {
            try {
                // Read one message from Corfu log.
                snapshotReadMessage = snapshotReader.read(snapshotSyncEventId);

                // Mark the readingCompleted if there is no more messages.
                readingCompleted = snapshotReadMessage.isEndRead();

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

            messagesSent += processReads(snapshotReadMessage.getMessages(), snapshotSyncEventId);
            observedCounter.setValue(messagesSent);
        }

       if (!cancel && !stopSnapshotSync) {
            // Maximum number of batch messages sent. This snapshot sync needs to continue.
            // Snapshot Sync is not performed in a single run, as for the case of multi-site replication
            // the shared thread pool could be lower than the number of sites, so we assign resources in
            // a round robin fashion.
            log.trace("Snapshot sync continue for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
            fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                    new LogReplicationEventMetadata(snapshotSyncEventId)));
       } else {
           log.warn("Snapshot sync stopped for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
       }
    }

    /**
     *
     * @param snapshotSyncEventId
     */
    public void querySnapshotSyncStatus(UUID snapshotSyncEventId) {
        if (!readingCompleted) {
            log.error("It is in the wrong state, the reading is not in the complete state {}", readingCompleted);
            return;
        }

        try {
            // Query receiver status
            LogReplicationQueryMetadataResponse response = dataSender.sendQueryMetadataRequest().get();

            // If it has finished applying the snapshot, transition to log entry sync
            // Otherwise query the status in another cycle.
            if (response.getLastLogProcessed() == response.getSnapshotApplied() &&
                response.getSnapshotApplied() == baseSnapshotTimestamp) {
                log.info("SNAPSHOT full sync complete according to response {}", response);
                snapshotSyncComplete(snapshotSyncEventId);
            } else {
                log.info("The receiver hasn't finished applying the snapshot yet {}", response);
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } catch (Exception e) {
            // There is a network issue or the receiver refuse to reply the message
            // Todo xiaoqin rediscovery the remote site.
        }
    }

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {
        log.info("Running snapshot sync for {} on baseSnapshot {}", snapshotSyncEventId,
                baseSnapshotTimestamp);

        if (!Address.isAddress(baseSnapshotTimestamp)) {
            log.warn("The baseSnapshotTimestamp is not a correct address and will ingore the snapshot sync reqeust.", baseSnapshotTimestamp);
            return;
        }

        if (stopSnapshotSync) {
            log.warn("Snapshot sync stopped for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
            return;
        }

        //Resend messages in buffer and process the ACKs.
        dataSenderBufferManager.resend();

        if (!readingCompleted) {
            log.info("Reading and sending phase.");
            readAndTransmit(snapshotSyncEventId);
        } else if (!dataSenderBufferManager.pendingMessages.isEmpty()) {
            log.info("Snapshot full sync: snapshot reader is done and the message buffer size {}",
                    dataSenderBufferManager.pendingMessages.getSize());
            fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                    new LogReplicationEventMetadata(snapshotSyncEventId)));
        } else {
            // It has finished reading the snapshot data and all data has been sent over and ACKed.
            log.info("Snapshot sync transfer has readingCompleted for {} as there is no data in the buffer.", snapshotSyncEventId);
            try {
                sleep(SLEEP_INTERVAL);
            } catch (Exception e) {
                log.warn("Caught an exception during sleep ", e);
            }
            querySnapshotSyncStatus(snapshotSyncEventId);
        }
    }

    /**
     * All snapshot messages are pass through the buffer.
     * @param logReplicationEntries
     * @param snapshotSyncEventId
     * @return
     */
    private int processReads(List<LogReplicationEntry> logReplicationEntries, UUID snapshotSyncEventId) {
        int numMessages = 0;

        // If we are starting a snapshot sync, send a start marker.
        if (startSnapshotSync) {
            LogReplicationEntry entry = getSnapshotSyncStartMarker(snapshotSyncEventId);
            entry.getMetadata().setSnapshotSyncSeqNum(Address.NON_ADDRESS);
            dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
            startSnapshotSync = false;
            numMessages++;
        }

        dataSenderBufferManager.sendWithBuffering(logReplicationEntries);

        // If Snapshot is complete, add end marker
        if (readingCompleted) {
            LogReplicationEntry endDataMessage = getSnapshotSyncEndMarker(snapshotSyncEventId);
            log.info("SnapshotSender sent out SNAPSHOT_TRANSSFER_END message {} ", endDataMessage.getMetadata());
            dataSenderBufferManager.sendWithBuffering(endDataMessage);
            numMessages++;
        }

        return numMessages + logReplicationEntries.size();
    }


    /**
     * Prepare a Snapshot Sync Replication start marker.
     * This message will has seqNum -1.
     * @param snapshotSyncEventId snapshot sync event identifier
     * @return snapshot sync start marker as LogReplicationEntry
     */
    private LogReplicationEntry getSnapshotSyncStartMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_START, fsm.getTopologyConfigId(),
                snapshotSyncEventId, Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata, new byte[0]);
        return emptyEntry;
    }

    /**
     * Prepare a Snapshot Sync Replication end marker.
     * This is the last message sent to the receiver. When receiving an ACKed for this message,
     * the sender will transit to polling receiver's status state.
     * @param snapshotSyncEventId
     * @return
     */
    private LogReplicationEntry getSnapshotSyncEndMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_END, fsm.getTopologyConfigId(), snapshotSyncEventId,
                Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata, new byte[0]);
        return emptyEntry;
    }


    /**
     * Complete Snapshot Sync, insert completion event in the FSM queue.
     *
     * @param snapshotSyncEventId unique identifier for the readingCompleted snapshot sync.
     */
    public void snapshotSyncComplete(UUID snapshotSyncEventId) {
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

        // Starting a new snapshot sync, reset the log reader's snapshot timestamp
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
