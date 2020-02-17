package org.corfudb.logreplication;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.MessageMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.ReplicationWriterException;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
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
    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;
    private DataSender dataSender;
    private DataControl dataControl;
    private LogReplicationConfig config;
    private boolean startSnapshot;
    private UUID snapshotRequestId;

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
        this.rxState = RxState.IDLE_STATE;
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
        startSnapshot = true;
    }

    /**
     * The end of snapshot sync
     */
    public void completeSnapshotApply() {
        //check if the all the expected message has received
        rxState = RxState.LOG_SYNC;
        persistedWriterMetadata.setsrcBaseSnapshotDone();

        // Prepare Snapshot Sync ACK
        MessageMetadata metadata = new MessageMetadata(MessageType.SNAPSHOT_REPLICATED,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotRequestId);

        dataSender.send(new DataMessage(metadata), snapshotRequestId, true);
    }

    @Override
    public void receive(DataMessage message) {
        // @maxi, how do we distinguish log entry apply from snapshot apply?
        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                if (startSnapshot && rxState == RxState.SNAPSHOT_SYNC) {
                    // If we are just starting, initialize
                    persistedWriterMetadata.setsrcBaseSnapshotStart(message.getMetadata().getSnapshotTimestamp());

                    // Signal start of snapshot sync to the receive, so data can be cleared.
                    this.snapshotWriter.reset(message.getMetadata().getSnapshotTimestamp());

                    snapshotRequestId = message.getMetadata().getSnapshotRequestId();

                    // Reset start snapshot flag / continue on this baseSnapshotTimestamp
                    startSnapshot = false;
                }

                if (rxState == RxState.SNAPSHOT_SYNC &&
                        message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_MESSAGE) {
                    this.snapshotWriter.apply(message);
                } else if (rxState == RxState.LOG_SYNC &&
                    message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE) {
                    long ackTs = this.logEntryWriter.apply(message);
                    if (ackTs > persistedWriterMetadata.getLastProcessedLogTimestamp()) {
                        persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());

                        MessageMetadata metadata = new MessageMetadata(MessageType.LOG_ENTRY_REPLICATED, ackTs, message.getMetadata().getSnapshotTimestamp());
                        DataMessage ack = new DataMessage(metadata);
                        dataSender.send(ack);

                        dataSender.send(message);
                    }
                } else {
                    log.error("it is in the wrong state {}", rxState + " messageType: " + message.getMetadata().getMessageMetadataType());
                    throw new ReplicationWriterException("wrong state");
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                dataControl.requestSnapshotSync();
                log.info("Requested Snapshot Sync.");
            }
        } else {
            log.warn("Set LogReplicationConfig for Sync");
        }
    }

    @Override
    public void receive(List<DataMessage> messages) {
        for (DataMessage msg : messages) {
            receive(msg);
        }
    }

    enum RxState {
        IDLE_STATE,
        SNAPSHOT_SYNC,
        LOG_SYNC
    };
}
