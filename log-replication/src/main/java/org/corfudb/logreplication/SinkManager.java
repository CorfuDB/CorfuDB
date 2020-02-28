package org.corfudb.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.message.LogReplicationEntryMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.ReplicationWriterException;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.runtime.CorfuRuntime;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.corfudb.logreplication.send.LogEntrySender.*;

/**
 * This class represents the Log Replication Manager at the destination.
 *
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class SinkManager implements DataReceiver {
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";

    private int ackCycleTime = DEFAULT_RESENT_TIMER/DEFAULT_MAX_RETRY;
    private int ackCycleCnt = DEFAULT_READER_QUEUE_SIZE;

    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;
    private DataSender dataSender;
    private DataControl dataControl;
    private LogReplicationConfig config;
    private UUID snapshotRequestId = new UUID(0L, 0L);

    private int rxMessageCounter = 0;
    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private ObservableValue rxMessageCount = new ObservableValue(rxMessageCounter);

    private int ackCnt = 0;
    private long ackTime = 0;

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
        this.runtime =  CorfuRuntime.fromParameters(rt.getParameters()).connect();
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
        readConfig();
    }

    private void readConfig() {
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            int logWriterQueueSize = Integer.parseInt(props.getProperty("log_writer_queue_size", Integer.toString(DEFAULT_READER_QUEUE_SIZE)));
            logEntryWriter.setMaxMsgQueSize(logWriterQueueSize);

            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(DEFAULT_READER_QUEUE_SIZE)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(DEFAULT_RESENT_TIMER)));
            reader.close();
            log.info("log writer config queue size {} ackCycleCnt {} ackCycleTime {}",
                    logWriterQueueSize, ackCycleCnt, ackCycleTime);
        } catch (Exception e) {
            log.warn("Caught an exception while reading the config file: {}", e.getCause());
        }
    }
    /**
     * Signal the manager a snapshot sync is about to start. This is required to reset previous states.
     */
    public void startSnapshotApply() {
        log.debug("Start of a snapshot apply");
        rxState = RxState.SNAPSHOT_SYNC;
    }

    /**
     * The end of snapshot sync
     */
    public void completeSnapshotApply() {
        log.debug("Complete of a snapshot apply");
        //check if the all the expected message has received
        rxState = RxState.LOG_SYNC;
        persistedWriterMetadata.setsrcBaseSnapshotDone();

        // Prepare and Send Snapshot Sync ACK
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_REPLICATED,
                snapshotRequestId,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotRequestId);

        dataSender.send(DataMessage.generateAck(metadata), snapshotRequestId, true);
    }

    @Override
    public void receive(DataMessage dataMessage) {

        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        if (log.isTraceEnabled()) {
            log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);
        }

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

    private boolean shouldAck() {
        ackCnt++;
        long currentTime = java.lang.System.currentTimeMillis();
        if (ackCnt == ackCycleCnt || (currentTime - ackTime) >= ackCycleTime) {
            ackCnt = 0;
            ackTime = currentTime;
            return true;
        }

        return false;
    }

    private void applyLogEntrySync(LogReplicationEntry message) {
        // Apply log entry sync message
        long ackTs = logEntryWriter.apply(message);

        // Send Ack for log entry
        if (ackTs > persistedWriterMetadata.getLastProcessedLogTimestamp()) {
            persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());

            if (shouldAck()) {
                // Prepare ACK message for Log Entry Sync
                LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.LOG_ENTRY_REPLICATED,
                        message.getMetadata().getSyncRequestId(), ackTs,
                        message.getMetadata().getSnapshotTimestamp());
                dataSender.send(DataMessage.generateAck(metadata));
            }
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

        log.debug("Received snapshot sync start marker for {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        // If we are just starting snapshot sync, initialize base snapshot start
        persistedWriterMetadata.setsrcBaseSnapshotStart(entry.getMetadata().getSnapshotTimestamp());

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(entry.getMetadata().getSnapshotTimestamp());

        // Retrieve snapshot request ID to be used for ACK of snapshot sync complete
        snapshotRequestId = entry.getMetadata().getSyncRequestId();
    }

    private boolean receivedValidMessage(LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_START)
                || rxState == RxState.LOG_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    @Override
    public void receive(List<DataMessage> messages) {
        for (DataMessage msg : messages) {
            receive(msg);
        }
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_SYNC
    }
}
