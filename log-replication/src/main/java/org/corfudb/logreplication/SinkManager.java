package org.corfudb.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.message.LogReplicationEntryMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.ReplicationWriterException;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.logreplication.send.LogEntrySender;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;
import java.util.UUID;

/**
 * This class represents the Log Replication Manager at the destination.
 *
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class SinkManager implements DataReceiver {
    private static int ACK_PERIOD = LogEntrySender.ENTRY_RESENT_TIMER/5;
    private static int ACK_CNT = LogEntrySender.READ_BATCH_SIZE/5;

    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;
    private DataSender dataSender;
    private DataControl dataControl;
    private LogReplicationConfig config;
    private UUID snapshotRequestId = new UUID(0L, 0L);

    /**
     * Constructor
     *
     * @param rt Corfu Runtime
     * @param config log replication configuration
     */
    public SinkManager(CorfuRuntime rt, DataSender dataSender, DataControl dataControl, LogReplicationConfig config) {
        this(rt, dataSender, dataControl);
        setLogReplicationConfig(config);
    }

    /**
     * Constructor
     *
     * This is temp to solve a dependency issue in application (to be removed as config is required)
     * This requires setLogReplicationConfig to be called.
     *
     * @param rt Corfu Runtime
     */
    public SinkManager(CorfuRuntime rt, DataSender dataSender, DataControl dataControl) {
        this.runtime = rt;
        this.rxState = RxState.LOG_SYNC;
        this.dataSender = dataSender;
        this.dataControl = dataControl;
    }

    public void setLogReplicationConfig(LogReplicationConfig config) {
        this.config = config;
        snapshotWriter = new StreamsSnapshotWriter(runtime, config);
        logEntryWriter = new LogEntryWriter(runtime, config);
        persistedWriterMetadata = new PersistedWriterMetadata(runtime, config.getRemoteSiteID());
        logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());
    }

    /**
     * Signal the manager a snapshot sync is about to start. This is required to reset previous states.
     */
    public void startSnapshotApply() {
        rxState = RxState.SNAPSHOT_SYNC;
    }

    /**
     * The end of snapshot sync
     */
    public void completeSnapshotApply() {
        //check if the all the expected message has received
        rxState = RxState.LOG_SYNC;
        persistedWriterMetadata.setsrcBaseSnapshotDone();

        // Prepare and Send Snapshot Sync ACK
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_REPLICATED,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotRequestId);

        dataSender.send(DataMessage.generateAck(metadata), snapshotRequestId, true);
    }

    @Override
    public void receive(DataMessage dataMessage) {
        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                // Convert DataMessage to Corfu Internal Message
                LogReplicationEntry message = LogReplicationEntry.deserialize(dataMessage.getData());

                if (!receivedValidMessage(message)) {
                    // Invalid message // Drop the message
                    log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                            message.getMetadata().getMessageMetadataType());
                    return;
                }

                if (rxState == RxState.SNAPSHOT_SYNC) {
                    applySnapshotSync(message);
                } else if (rxState == RxState.LOG_SYNC) {
                    applyLogEntrySync(message);
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                dataControl.requestSnapshotSync();
                log.info("Requested Snapshot Sync.");
            }
        } else {
            log.error("Required LogReplicationConfig for Sink Manager.");
            throw new IllegalArgumentException("Required LogReplicationConfig for Sink Manager.");
        }
    }

    private void applyLogEntrySync(LogReplicationEntry message) {
        // Apply log entry sync message
        long ackTs = logEntryWriter.apply(message);

        // Send Ack for log entry
        if (ackTs > persistedWriterMetadata.getLastProcessedLogTimestamp()) {
            persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());

            // TODO: this will be changed to be sent every T seconds instead on every apply
            // Prepare ACK message for Log Entry Sync
            LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.LOG_ENTRY_REPLICATED, ackTs,
                    message.getMetadata().getSnapshotTimestamp());
            dataSender.send(DataMessage.generateAck(metadata));
        }
    }

    private void applySnapshotSync(LogReplicationEntry message) {
        // If the snapshot sync has just started, initialize snapshot sync metadata
        if (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_START) {
            initializeSnapshotSync(message);
        } else {
            // For all other snapshot sync messages, apply
            snapshotWriter.apply(message);
        }
    }

    private void initializeSnapshotSync(LogReplicationEntry entry) {
        // If we are just starting snapshot sync, initialize base snapshot start
        persistedWriterMetadata.setsrcBaseSnapshotStart(entry.getMetadata().getSnapshotTimestamp());

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(entry.getMetadata().getSnapshotTimestamp());

        // Retrieve snapshot request ID to be used for ACK of snapshot sync complete
        snapshotRequestId = entry.getMetadata().getSnapshotRequestId();
    }

    private boolean receivedValidMessage(LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_START)
                || rxState == RxState.LOG_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    @Override
    public void receive(List<DataMessage> messages) {
        LogEntrySender.DefaultTimer timer = new LogEntrySender.DefaultTimer();
        for (DataMessage msg : messages) {
            receive(msg);
        }
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_SYNC
    }
}
