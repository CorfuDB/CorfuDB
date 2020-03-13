package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_MESSAGE;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_MESSAGE;

/**
 * This class represents the Log Replication Manager at the destination.
 *
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class SinkManager implements DataReceiver {
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";

    public static final int DEFAULT_READER_QUEUE_SIZE = 1;
    public static final int DEFAULT_RESENT_TIMER = 5000;
    public static final int DEFAULT_MAX_RETRY = 5;

    private int ackCycleTime = DEFAULT_RESENT_TIMER/ DEFAULT_MAX_RETRY;
    private int ackCycleCnt = DEFAULT_READER_QUEUE_SIZE;
    private int bufferSize = DEFAULT_READER_QUEUE_SIZE;

    private CorfuRuntime runtime;
    private SinkBufferManager sinkBufferManager;
    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;

    private LogReplicationConfig config;
    private UUID snapshotRequestId = new UUID(0L, 0L);

    private int rxMessageCounter = 0;
    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private ObservableValue rxMessageCount = new ObservableValue(rxMessageCounter);

    /**
     * Constructor
     *
     * This is temp to solve a dependency issue in application (to be removed as config is required)
     * This requires setLogReplicationConfig to be called.
     *
     * @param rt Corfu Runtime
     */
    public SinkManager(CorfuRuntime rt, LogReplicationConfig config) {
        CorfuRuntime dedicatedRuntime = CorfuRuntime.fromParameters(rt.getParameters());
        dedicatedRuntime.parseConfigurationString(rt.getLayoutServers().get(0)).connect();
        this.runtime =  dedicatedRuntime;
        this.rxState = RxState.LOG_SYNC;
        setLogReplicationConfig(config);
    }

    public SinkManager(String localCorfuEndpoint, LogReplicationConfig config) {
        this(CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build()).parseConfigurationString(localCorfuEndpoint), config);
    }

    private void setLogReplicationConfig(LogReplicationConfig config) {
        this.config = config;
        persistedWriterMetadata = new PersistedWriterMetadata(runtime, config.getRemoteSiteID());

        snapshotWriter = new StreamsSnapshotWriter(runtime, config, persistedWriterMetadata);
        logEntryWriter = new LogEntryWriter(runtime, config, persistedWriterMetadata);
        logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());
        readConfig();
        sinkBufferManager = new SinkBufferManager(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, bufferSize,
                persistedWriterMetadata.getLastProcessedLogTimestamp(), this);
    }

    private void readConfig() {
        try {
            File configFile = new File(config_file);
            FileReader reader = new FileReader(configFile);

            Properties props = new Properties();
            props.load(reader);

            bufferSize = Integer.parseInt(props.getProperty("log_writer_queue_size", Integer.toString(DEFAULT_READER_QUEUE_SIZE)));
            logEntryWriter.setMaxMsgQueSize(bufferSize);

            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(DEFAULT_READER_QUEUE_SIZE)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(DEFAULT_RESENT_TIMER)));
            reader.close();
            log.info("log writer config queue size {} ackCycleCnt {} ackCycleTime {}",
                    bufferSize, ackCycleCnt, ackCycleTime);
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
        // If we don't use AR, we need our own buffer
        // sinkBufferManager = new SinkBufferManager(SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, bufferSize);
    }

    /**
     * The end of snapshot sync
     */
    public LogReplicationEntry completeSnapshotApply() {
        log.debug("Complete of a snapshot apply");
        //check if the all the expected message has received
        rxState = RxState.LOG_SYNC;
        persistedWriterMetadata.setSrcBaseSnapshotDone();

        // Prepare and Send Snapshot Sync ACK
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_REPLICATED,
                snapshotRequestId,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotRequestId);

        sinkBufferManager = new SinkBufferManager(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, bufferSize,
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(), this);

        return LogReplicationEntry.generateAck(metadata);
    }

    @Override
    public LogReplicationEntry receive(LogReplicationEntry dataMessage) {
        if (rxState == RxState.LOG_SYNC) {
            return sinkBufferManager.receive(dataMessage);
        } else {
            receiveWithoutBuffering(dataMessage);
        }

        // TODO: change this
        return LogReplicationEntry.generateAck(dataMessage.getMetadata());
    }

    public void receiveWithoutBuffering(LogReplicationEntry message) {

        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        if (log.isTraceEnabled()) {
            log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);
        }

        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                if (!receivedValidMessage(message)) {
                    // If we received a start marker for snapshot sync, switch state
                    if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_START)) {
                        startSnapshotApply();
                    } else {
                        // Invalid message // Drop the message
                        log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                                message.getMetadata().getMessageMetadataType());
                        return;
                    }
                }

                if (rxState == RxState.SNAPSHOT_SYNC) {
                    applySnapshotSync(message);
                } else if (rxState == RxState.LOG_SYNC) {
                    applyLogEntrySync(message);
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                // TODO: Let ack time out which will kick off Snapshot Sync or send a NACK?
                log.info("Requested Snapshot Sync.");
            }
        } else {
            log.error("Required LogReplicationConfig for Sink Manager.");
            throw new IllegalArgumentException("Required LogReplicationConfig for Sink Manager.");
        }
    }

    private void applyLogEntrySync(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry message) {
        // Apply log entry sync message
        long ackTs = logEntryWriter.apply(message);

        // Send Ack for log entry
        //if (ackTs > persistedWriterMetadata.getLastProcessedLogTimestamp()) {
        //    persistedWriterMetadata.setLastProcessedLogTimestamp(message.metadata.getTimestamp());
        //}
    }

    private void applySnapshotSync(LogReplicationEntry message) {

        switch (message.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_START:
                initializeSnapshotSync(message);
                break;
            case SNAPSHOT_MESSAGE:
                snapshotWriter.apply(message);
                break;
            case SNAPSHOT_END:
                completeSnapshotApply();
                break;
            default:
                log.warn("Message type {} should not be applied as snapshot sync.", message.getMetadata().getMessageMetadataType());
                break;
        }
    }

    private void initializeSnapshotSync(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry) {
        long timestamp = entry.getMetadata().getSnapshotTimestamp();

        log.debug("Received snapshot sync start marker for {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        // If we are just starting snapshot sync, initialize base snapshot start
        timestamp = persistedWriterMetadata.setSrcBaseSnapshotStart(timestamp);

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(timestamp);

        // Retrieve snapshot request ID to be used for ACK of snapshot sync complete
        snapshotRequestId = entry.getMetadata().getSyncRequestId();
    }

    private boolean receivedValidMessage(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_START || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END)
                || rxState == RxState.LOG_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    public void shutdown() {
        this.runtime.shutdown();
    }

    @Override
    public List<LogReplicationEntry> receive(List<LogReplicationEntry> messages) {
        for (LogReplicationEntry msg : messages) {
            receive(msg);
        }

        // TODO: fix this
        return Collections.emptyList();
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_SYNC
    }
}
