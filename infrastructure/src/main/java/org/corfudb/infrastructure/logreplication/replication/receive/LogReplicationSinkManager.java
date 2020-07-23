package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.immutables.value.internal.$guava$.annotations.$VisibleForTesting;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_END;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_MESSAGE;

/**
 * This class represents the Log Replication Manager at the destination.
 * It is the entry point for log replication at the receiver.
 *
 * */
@Slf4j
public class LogReplicationSinkManager implements DataReceiver {
    /*
     * Read SinkManager configuration information from a file.
     * If the file is not available, use the default values.
     */
    private static final String config_file = "/config/corfu/corfu_replication_config.properties";

    /*
     * how long in milliseconds a ACK sent back to sender
     */
    private int ackCycleTime;

    /*
     * how frequent a ACK sent back to sender
     */
    private int ackCycleCnt;
    private int bufferSize;

    private CorfuRuntime runtime;

    private LogEntrySinkBufferManager logEntrySinkBufferManager;
    private SnapshotSinkBufferManager snapshotSinkBufferManager;

    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;
    private RxState rxState;

    // The executor is used to execute the applying phase for full snapshot sync.
    private final ExecutorService applySnapshotExecutor;

    private LogReplicationConfig config;

    /*
     * the current baseSnapshot
     */
    private long baseSnapshotTimestamp = Address.NON_ADDRESS - 1;

    /*
     * current topologyConfigId, used to drop out of date messages.
     */
    private long topologyConfigId = 0;

    @$VisibleForTesting
    private int rxMessageCounter = 0;

    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private ObservableValue rxMessageCount = new ObservableValue(rxMessageCounter);

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     * @param metadataManager
     * @param context
     */
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager,
                                     ServerContext context, long topologyConfigId) {
        this.logReplicationMetadataManager = metadataManager;
        this.applySnapshotExecutor = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("apply-snapshot-executor").build());


        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .trustStore((String) context.getServerConfig().get("--truststore"))
                .tsPasswordFile((String) context.getServerConfig().get("--truststore-password-file"))
                .keyStore((String) context.getServerConfig().get("--keystore"))
                .ksPasswordFile((String) context.getServerConfig().get("--keystore-password-file"))
                .tlsEnabled((Boolean) context.getServerConfig().get("--enable-tls"))
                .build())
                .parseConfigurationString(localCorfuEndpoint).connect();

        /*
         * When the server is up, it will be at LOG_ENTRY_SYNC state by default.
         * The sender will query receiver's status and decide what type of replication to start with.
         * It will transit to SNAPSHOT_SYNC state if it received a SNAPSHOT_START message from the sender.
         */
        this.rxState = RxState.LOG_ENTRY_SYNC;
        this.config = config;
        this.topologyConfigId = topologyConfigId;
        init();
    }

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     */
    @VisibleForTesting
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager) {
        this.logReplicationMetadataManager = metadataManager;
        this.applySnapshotExecutor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationSinkManager-", new ServerThreadFactory.ExceptionHandler()));

        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(localCorfuEndpoint).connect();

        /*
         * When the server is up, it will be at LOG_ENTRY_SYNC state by default.
         * The sender will query receiver's status and decide what type of replication to start with.
         * It will transit to SNAPSHOT_SYNC state if it received a SNAPSHOT_START message from the sender.
         */
        this.rxState = RxState.LOG_ENTRY_SYNC;
        this.config = config;

        init();
    }

    /**
     * Init variables.
     */
    private void init() {
        snapshotWriter = new StreamsSnapshotWriter(runtime, config, logReplicationMetadataManager);
        logEntryWriter = new LogEntryWriter(runtime, config, logReplicationMetadataManager);
        logEntryWriter.reset(logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp(),
                logReplicationMetadataManager.getLastProcessedLogTimestamp());

        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,this);

        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize, this);

        readConfig();
    }

    /**
     * Read the SinkManager configuration, such as buffer size and how frequent to send ACKs.
     * With changing this config file, we can do more testing to find the most optimal's way to for the setup.
     * If the configFile doesn't exist, use the default values.
     */
    private void readConfig() {
        File configFile = new File(config_file);
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            bufferSize = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(bufferSize)));
            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(ackCycleCnt)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(ackCycleTime)));
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", config_file);
        } catch (IOException e) {
            log.error("IO Exception when reading config file", e);
        }

        log.info("Sink Manager Buffer config queue size {} ackCycleCnt {} ackCycleTime {}",
                bufferSize, ackCycleCnt, ackCycleTime);
    }

    /**
     * Receive a message from the sender.
     *
     * @param message
     * @return
     */
    @Override
    public LogReplicationEntry receive(LogReplicationEntry message) {
        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        log.debug("Sink manager received {} while in {}", message.getMetadata().getMessageMetadataType(), rxState);

         // Ignore messages that have different topologyConfigId.
         // It could be caused by an out-of-date sender or the local node hasn't done the site discovery yet.
         // If there is a siteConfig change, the discovery service will detect it and reset the state.
        if (message.getMetadata().getTopologyConfigId() != topologyConfigId) {
            log.warn("Sink manager with config id {} ignored msg id {}", topologyConfigId,
                    message.getMetadata().getTopologyConfigId());
            return null;
        }

        // If it receives a SNAPSHOT_START message, prepare a transition
        if (message.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_START)) {
            processSnapshotStart(message);
            return null;
        }

        if (!receivedValidMessage(message)) {

            // It is possible that the sender doesn't receive the SNAPSHOT_END ACK message and
            // send the SNAPSHOT_END message again, but the receiver has already transited to
            // the LOG_ENTRY_SYNC state.
            // Reply the SNAPSHOT_ACK again and let sender do the proper transition.
            if (message.getMetadata().getMessageMetadataType() == SNAPSHOT_END) {
                log.warn("Sink Manager in state {} and received message {}. Resending the ACK for SNAPSHOT_END.", rxState,
                        message.getMetadata());
                LogReplicationEntryMetadata metadata = snapshotSinkBufferManager.getAckMetadata(message);
                return new LogReplicationEntry(metadata, new byte[0]);
            }

            // Invalid message and drop it.
            log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                    message.getMetadata());

            return null;
        }

        if (rxState.equals(RxState.LOG_ENTRY_SYNC)) {
            return logEntrySinkBufferManager.processMsgAndBuffer(message);
        } else {
            return snapshotSinkBufferManager.processMsgAndBuffer(message);
        }
    }

    /**
     * Process a SNAPSHOT_START message. This message will not be pushed to the buffer,
     * as it trigger a transition and reset the state.
     * If it is requesting a new snapshot with higher timestamp, transition to SNAPSHOT_SYNC state,
     * otherwise ignore the message.
     * @param entry
     */
    private void processSnapshotStart(LogReplicationEntry entry) {
        long topologyConfigId = entry.getMetadata().getTopologyConfigId();
        long timestamp = entry.getMetadata().getSnapshotTimestamp();

        log.info("Received snapshot sync start marker for {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        /*
         * It is out of date message due to resend, drop it.
         */
        if (entry.getMetadata().getSnapshotTimestamp() < baseSnapshotTimestamp) {
            // Invalid message and drop it.
            log.warn("Sink Manager in state {} and received message {}. " +
                            "Dropping Message due to snapshotTimestamp is smaller than the current one {}",
                    rxState, entry.getMetadata(), baseSnapshotTimestamp);
            return;
        }

        /*
         * Fails to set the baseSnapshot at the metadata store, it could be a out of date message,
         * or the current node is out of sync, ignore it.
         */
        if (logReplicationMetadataManager.setSrcBaseSnapshotStart(topologyConfigId, timestamp) == false) {
            log.warn("Sink Manager in state {} and received message {}. " +
                            "Dropping Message due to failure update of the metadata store {}",
                    rxState, entry.getMetadata(), logReplicationMetadataManager);
            return;
        }

        /*
         * Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
         */
        snapshotWriter.reset(topologyConfigId, timestamp);

        // Update lastTransferDone with the new snapshot transfer timestamp.
        baseSnapshotTimestamp = entry.getMetadata().getSnapshotTimestamp();

        // Setup buffer manager.
        snapshotSinkBufferManager.reset();

        // Set state in SNAPSHOT_SYNC state.
        rxState = RxState.SNAPSHOT_SYNC;
        log.info("Sink manager entry {} state, snapshot start with {}", rxState, entry.getMetadata());
    }

    /**
     * Signal the manager a snapshot sync is about to complete. This is required to transition to log sync.
     */
    private void completeSnapshotApply(LogReplicationEntry inputEntry) {
        log.debug("Complete of a snapshot apply");
        rxState = RxState.LOG_ENTRY_SYNC;
        logEntrySinkBufferManager.reset();
        log.info("Sink manager completed SNAPSHOT transfer for {} and has transit to {} state.",
                inputEntry, rxState);
    }

    /**
     * Process a snapshot transfer end marker:
     * 1. Mark the metadata that snapshot transfer phase is done.
     * 2. Start to apply the snapshot data to the real streams
     * 3. After applying, transit to log entry sync state.
     * @param message
     */
    private void processSnapshotTransferEndMarker(LogReplicationEntry message) {
        snapshotWriter.setSnapshotTransferDoneAndStartApply(message);
        completeSnapshotApply(message);
    }

    /**
     * Process SNAPSHOT transfer messages
     * @param message
     */
    private void applySnapshotSync(LogReplicationEntry message) {
        switch (message.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
                snapshotWriter.apply(message);
                return;

            case SNAPSHOT_END:
                // Submit a job to the executor
                applySnapshotExecutor.submit(() -> processSnapshotTransferEndMarker(message));

                return;

            default:
                log.warn("Message type {} should not be applied as snapshot sync.", message.getMetadata().getMessageMetadataType());
                break;
        }
    }

    /**
     * Query metadata corfu table and get the most recent information.
     * @return
     */
    public LogReplicationQueryMetadataResponse processQueryMetadataRequest() {
        LogReplicationQueryMetadataResponse response = new LogReplicationQueryMetadataResponse(
                logReplicationMetadataManager.getTopologyConfigId(),
                logReplicationMetadataManager.getVersion(),
                logReplicationMetadataManager.getLastSnapStartTimestamp(),
                logReplicationMetadataManager.getLastSnapTransferDoneTimestamp(),
                logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp(),
                logReplicationMetadataManager.getLastProcessedLogTimestamp());
        log.trace("Query metadata response {}", response);
        return response;
    }

    /**
     * While processing an in order message, the buffer will callback and process the message
     * @param message
     */
    public void processMessage(LogReplicationEntry message) {
        log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);

        switch (rxState) {
            case LOG_ENTRY_SYNC:
                logEntryWriter.apply(message);
                break;

            case SNAPSHOT_SYNC:
                applySnapshotSync(message);
                break;

            default:
                log.error("Wrong state {}.", rxState);
        }
    }

    /*
     * Verify if the message is the correct type for the current state.
     * @param message
     * @return
     */
    private boolean receivedValidMessage(LogReplicationEntry message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getMessageMetadataType() == SNAPSHOT_MESSAGE
                || message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_END)
                || rxState == RxState.LOG_ENTRY_SYNC && message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_MESSAGE;
    }

    /**
     * Update the topology config id
     *
     * @param topologyConfigId
     */
    public void updateTopologyConfigId(long topologyConfigId) {
        logReplicationMetadataManager.setupTopologyConfigId(topologyConfigId);
        this.topologyConfigId = topologyConfigId;
    }

    /**
     * When there is a cluster role change, the Sink Manager needs do the following:
     *
     * 1. Reset snapshotWriter and logEntryWriter state
     * 2. Reset buffer logEntryBuffer state.
     *
     * */
    public void reset() {
        snapshotWriter.reset(topologyConfigId, logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp());
        logEntryWriter.reset(logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp(),
                logReplicationMetadataManager.getLastProcessedLogTimestamp());

        logEntrySinkBufferManager.reset();
    }

    public void shutdown() {
        this.runtime.shutdown();
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}