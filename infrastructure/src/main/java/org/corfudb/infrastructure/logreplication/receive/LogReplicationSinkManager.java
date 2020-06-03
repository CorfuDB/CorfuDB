package org.corfudb.infrastructure.logreplication.receive;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DefaultSiteConfig;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_MESSAGE;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_REPLICATED;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_MESSAGE;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_REPLICATED;

/**
 * This class represents the Log Replication Manager at the destination.
 *
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class LogReplicationSinkManager implements DataReceiver {
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

    @Getter
    private PersistedWriterMetadata persistedWriterMetadata;
    private RxState rxState;

    @Getter
    @Setter
    boolean leader;

    // The roleType is active or standby
    @Getter
    @Setter
    private boolean active = false;

    private LogReplicationConfig config;
    private UUID snapshotRequestId = new UUID(0L, 0L);

    private int rxMessageCounter = 0;
    private long siteConfigID = 0;


    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private ObservableValue rxMessageCount = new ObservableValue(rxMessageCounter);

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     */
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config) {
        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(localCorfuEndpoint).connect();
        this.rxState = RxState.LOG_ENTRY_SYNC;
        this.config = config;
        init(LOG_ENTRY_MESSAGE);
    }

    private void init(MessageType type) {
        persistedWriterMetadata = new PersistedWriterMetadata(runtime, 0, config.getSiteID(), config.getRemoteSiteID());
        snapshotWriter = new StreamsSnapshotWriter(runtime, config, persistedWriterMetadata);
        logEntryWriter = new LogEntryWriter(runtime, config, persistedWriterMetadata);
        logEntryWriter.setTimestamp(persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                persistedWriterMetadata.getLastProcessedLogTimestamp());

        readConfig();
        sinkBufferManager = new SinkBufferManager(type, ackCycleTime, ackCycleCnt, bufferSize,
                Address.NON_ADDRESS, this);
    }

    private void readConfig() {
        File configFile = new File(config_file);
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);

            bufferSize = Integer.parseInt(props.getProperty("log_writer_queue_size"));
            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count"));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time"));
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", config_file);
            bufferSize = DefaultSiteConfig.getLogWriterQueueSize();
            ackCycleCnt = DefaultSiteConfig.getLogWriterAckCycleCount();
            ackCycleTime = DefaultSiteConfig.getLogWriterAckCycleTime();
        } catch (IOException e) {
            log.error("IO Exception when reading config file", e);
        }
        logEntryWriter.setMaxMsgQueSize(bufferSize);
        log.info("Log writer config queue size {} ackCycleCnt {} ackCycleTime {}",
                bufferSize, ackCycleCnt, ackCycleTime);
    }

    @Override
    public LogReplicationEntry receive(LogReplicationEntry message) {

        //If the roletype is active or it is not the leader, skip processing the message and ack the timestamp.
        //This will  trigger the active site to redo the query leadership.
        if (active || !leader) {
            LogReplicationEntryMetadata metadata = sinkBufferManager.makeAckMessage(message);
            return new LogReplicationEntry(metadata, new byte[0]);
        }

        log.info("Sink manager received {} while in {}", message.getMetadata().getMessageMetadataType(), rxState);

        siteConfigID = Math.max(siteConfigID, message.getMetadata().getSiteConfigID());

        if (!receivedValidMessage(message)) {
            // If we received a start marker for snapshot sync while in LOG_ENTRY_SYNC, switch rxState
            if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_START)) {
                rxState = RxState.SNAPSHOT_SYNC;
                init(SNAPSHOT_MESSAGE);
            } else {
                // Invalid message // Drop the message
                log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                        message.getMetadata());

                return null;
            }
        }

        if (rxState.equals(RxState.LOG_ENTRY_SYNC)) {
            return sinkBufferManager.processMsgAndBuffer(message);
        }

        // Snapshot Sync (no buffering, out of order is allowed)
        return receiveWithoutBuffering(message);
    }

    @Override
    public LogReplicationEntry receive(List<LogReplicationEntry> messages) {
        for (LogReplicationEntry msg : messages) {
            receive(msg);
        }

        // Todo (hack): ACK should be controlled from the receive method
        if (messages.size() > 0) {
            return LogReplicationEntry.generateAck(messages.get(0).getMetadata());
        }

        return null;
    }

    /**
     * Signal the manager a snapshot sync is about to complete. This is required to transition to log sync.
     */
    private LogReplicationEntry completeSnapshotApply(LogReplicationEntry inputEntry) {
        log.debug("Complete of a snapshot apply");
        //check if the all the expected message has received
        rxState = RxState.LOG_ENTRY_SYNC;
        persistedWriterMetadata.setSrcBaseSnapshotDone(inputEntry);

        // Prepare and Send Snapshot Sync ACK
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_END,
                siteConfigID,
                snapshotRequestId,
                persistedWriterMetadata.getLastProcessedLogTimestamp(),
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(),
                snapshotRequestId);

        sinkBufferManager = new SinkBufferManager(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, bufferSize,
                persistedWriterMetadata.getLastSrcBaseSnapshotTimestamp(), this);

        System.out.print("Sink manager send out snapEnd message ack " + metadata);
        return LogReplicationEntry.generateAck(metadata);
    }

    public LogReplicationEntry receiveWithoutBuffering(LogReplicationEntry message) {

        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        if (log.isTraceEnabled()) {
            log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);
        }

        // Buffer data (out of order) and apply
        if (config != null) {
            try {
                if (rxState == RxState.SNAPSHOT_SYNC) {
                    return applySnapshotSync(message);
                } else if (rxState == RxState.LOG_ENTRY_SYNC) {
                    logEntryWriter.apply(message);
                    // Log Entry ACK policy is handled by the caller
                    return null;
                }
            } catch (ReplicationWriterException e) {
                log.error("Get an exception: ", e);
                //System.out.print("\nGet an exception " + e);
                // TODO: Let ack time out which will kick off Snapshot Sync or send a NACK?
                log.info("Requested Snapshot Sync.");
            }
        } else {
            log.error("Required LogReplicationConfig for Sink Manager.");
            throw new IllegalArgumentException("Required LogReplicationConfig for Sink Manager.");
        }

        return null;
    }

    private LogReplicationEntry applySnapshotSync(LogReplicationEntry message) {
        switch (message.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_START:
                initializeSnapshotSync(message);
                System.out.print("\nSink manager snapshot start " + message.getMetadata());
                return message;
            case SNAPSHOT_MESSAGE:
                snapshotWriter.apply(message);
                message.getMetadata().setMessageMetadataType(SNAPSHOT_REPLICATED);
                return message;
            case SNAPSHOT_END:
                System.out.print("\nSink manager snapshot end " + message.getMetadata());
                snapshotWriter.snapshotTransferDone(message);
                return completeSnapshotApply(message);
            default:
                log.warn("Message type {} should not be applied as snapshot sync.", message.getMetadata().getMessageMetadataType());
                break;
        }

        return null;
    }

    private void initializeSnapshotSync(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry entry) {
        long siteConfigID = entry.getMetadata().getSiteConfigID();
        long timestamp = entry.getMetadata().getSnapshotTimestamp();

        log.debug("Received snapshot sync start marker for {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        // If we are just starting snapshot sync, initialize base snapshot start
        persistedWriterMetadata.setSrcBaseSnapshotStart(siteConfigID, timestamp);

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(siteConfigID, timestamp);

        // Retrieve snapshot request ID to be used for ACK of snapshot sync complete
        snapshotRequestId = entry.getMetadata().getSyncRequestId();
    }

    private boolean receivedValidMessage(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_START || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END)
                || rxState == RxState.LOG_ENTRY_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    public void setSiteInfo(boolean active, long siteID) {
        this.active = active;
        this.siteConfigID = siteID;
        persistedWriterMetadata.setupSiteConfigID(siteConfigID);
    }


    public void shutdown() {
        this.runtime.shutdown();
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}
