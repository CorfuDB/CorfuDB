package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;

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
    private static final String CONFIG_FILE = "/config/corfu/corfu_replication_config.properties";

    private static final int DEFAULT_ACK_CNT = 1;

    // Duration in milliseconds after which an ACK is sent back to the sender
    // if the message count is not reached before
    private int ackCycleTime = DEFAULT_ACK_CNT;

    // Number of messages received before sending a summarized ACK
    private int ackCycleCnt;

    private int bufferSize;

    private final CorfuRuntime runtime;

    private LogEntrySinkBufferManager logEntrySinkBufferManager;
    private SnapshotSinkBufferManager snapshotSinkBufferManager;

    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;
    private RxState rxState;

    private LogReplicationConfig config;

    private long baseSnapshotTimestamp = Address.NON_ADDRESS - 1;
    private UUID lastSnapshotSyncId = null;

    // Current topologyConfigId, used to drop out of date messages.
    private long topologyConfigId = 0;

    @VisibleForTesting
    private int rxMessageCounter = 0;

    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private final ObservableValue<Integer> rxMessageCount = new ObservableValue<>(rxMessageCounter);

    private ISnapshotSyncPlugin snapshotSyncPlugin;

    private final String pluginConfigFilePath;

    private ExecutorService applyExecutor;

    @Getter
    private final AtomicBoolean ongoingApply = new AtomicBoolean(false);

    private int waitMsBeforeSnapshotApply;

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

        this.runtime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .trustStore((String) context.getServerConfig().get(ConfigParamNames.TRUST_STORE))
                .tsPasswordFile((String) context.getServerConfig().get(ConfigParamNames.TRUST_STORE_PASS_FILE))
                .keyStore((String) context.getServerConfig().get(ConfigParamNames.KEY_STORE))
                .ksPasswordFile((String) context.getServerConfig().get(ConfigParamNames.KEY_STORE_PASS_FILE))
                .tlsEnabled((Boolean) context.getServerConfig().get("--enable-tls"))
                .maxCacheEntries(config.getMaxCacheSize())
                .maxWriteSize(context.getMaxWriteSize())
                .build())
                .parseConfigurationString(localCorfuEndpoint).connect();
        log.info("Runtime created from Sink Manager {}", runtime);
        this.pluginConfigFilePath = context.getPluginConfigFilePath();
        this.topologyConfigId = topologyConfigId;
        waitMsBeforeSnapshotApply = context.getSnapshotApplyWaitTime();
        init(metadataManager, config);
    }

    /**
     * Constructor Sink Manager
     *
     * @param localCorfuEndpoint endpoint for local corfu server
     * @param config log replication configuration
     */
    @VisibleForTesting
    public LogReplicationSinkManager(String localCorfuEndpoint, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager, String pluginConfigFilePath) {
        this.runtime =  CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .maxCacheEntries(config.getMaxCacheSize())
                .build())
                .parseConfigurationString(localCorfuEndpoint).connect();
        this.pluginConfigFilePath = pluginConfigFilePath;
        init(metadataManager, config);
    }

    /**
     * Initialize common parameters
     *
     * @param metadataManager metadata manager instance
     * @param config log replication configuration
     */
    private void init(LogReplicationMetadataManager metadataManager, LogReplicationConfig config) {
        this.logReplicationMetadataManager = metadataManager;
        this.config = config;

        // When the server is up, it will be at LOG_ENTRY_SYNC state by default.
        // The sender will query receiver's status and decide what type of replication to start with.
        // It will transit to SNAPSHOT_SYNC state if it received a SNAPSHOT_START message from the sender.
        this.rxState = RxState.LOG_ENTRY_SYNC;

        this.applyExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("snapshotSyncApplyExecutor")
                        .build());

        initWriterAndBufferMgr();
    }

    private void setDataConsistentWithRetry(boolean isDataConsistent) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    logReplicationMetadataManager.setDataConsistentOnStandby(isDataConsistent);
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to setDataConsistent in SinkManager's init", tae);
                    throw new RetryNeededException();
                }

                log.debug("setDataConsistentWithRetry succeeds, current value is {}", isDataConsistent);

                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to setDataConsistent in SinkManager's init.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Init the writers, Buffer Manager and Snapshot Plugin.
     */
    private void initWriterAndBufferMgr() {
        // Read config first before init other components.
        readConfig();

        // Instantiate Snapshot Sync Plugin, this is an external service which will be triggered on start and end
        // of a snapshot sync.
        snapshotSyncPlugin = getOnSnapshotSyncPlugin();

        snapshotWriter = new StreamsSnapshotWriter(runtime, config, logReplicationMetadataManager);
        logEntryWriter = new LogEntryWriter(config, logReplicationMetadataManager);

        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp(), this);
    }

    private ISnapshotSyncPlugin getOnSnapshotSyncPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getSnapshotSyncPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getSnapshotSyncPluginCanonicalName(), true, child);
            return (ISnapshotSyncPlugin) plugin.getDeclaredConstructor(CorfuRuntime.class)
                    .newInstance(runtime);
        } catch (Throwable t) {
            log.error("Fatal error: Failed to get snapshot sync plugin {}", config.getSnapshotSyncPluginCanonicalName(), t);
            throw new UnrecoverableCorfuError(t);
        }
    }

    /**
     * Read the SinkManager configuration, such as buffer size and how frequent to send ACKs.
     * With changing this config file, we can do more testing to find the most optimal's way to for the setup.
     * If the configFile doesn't exist, use the default values.
     */
    private void readConfig() {
        File configFile = new File(CONFIG_FILE);
        try {
            FileReader reader = new FileReader(configFile);
            Properties props = new Properties();
            props.load(reader);
            bufferSize = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(bufferSize)));
            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(ackCycleCnt)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(ackCycleTime)));
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", CONFIG_FILE);
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
    public LogReplication.LogReplicationEntryMsg receive(LogReplication.LogReplicationEntryMsg message) {
        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        log.debug("Sink manager received {} while in {}", message.getMetadata().getEntryType(), rxState);

        // Ignore messages that have different topologyConfigId.
        // It could be caused by an out-of-date sender or the local node hasn't done the site discovery yet.
        // If there is a siteConfig change, the discovery service will detect it and reset the state.
        if (message.getMetadata().getTopologyConfigID() != topologyConfigId) {
            log.warn("Drop message {}. Topology config id mismatch, local={}, msg={}", message.getMetadata().getEntryType(),
                    topologyConfigId, message.getMetadata().getTopologyConfigID());
            return null;
        }

        if (isMessageFromNewSnapshotSync(message) && ongoingApply.get()) {
            log.warn("Snapshot Apply for sync id {} is already ongoing.  Not accepting messages from a new Snapshot " +
                "Sync Cycle.  Dropping message {}", lastSnapshotSyncId, message);
            return null;
        }

        // If it receives a SNAPSHOT_START message, prepare a transition
        if (message.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_START)) {
            if (isValidSnapshotStart(message)) {
                processSnapshotStart(message);
                // The SnapshotPlugin will be called when LR is ready to start a snapshot sync,
                // so the system can prepare for the full sync. Typically, to stop checkpoint/trim
                // during the period of the snapshot sync to prevent data loss from shadow tables
                // (temporal non-checkpointed streams). This is a blocking call.
                log.info("Enter onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
                snapshotSyncPlugin.onSnapshotSyncStart(runtime);
                log.info("Exit onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
            }
            return null;
        }

        if (!receivedValidMessage(message)) {
            // It is possible that the sender doesn't receive the SNAPSHOT_TRANSFER_COMPLETE ack message and
            // sends the SNAPSHOT_END marker again, but the receiver has already transited to
            // the LOG_ENTRY_SYNC state.
            // In this case send the SNAPSHOT_TRANSFER_COMPLETE ack again so the sender can do the proper transition.
            if (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END) {
                LogReplicationEntryMetadataMsg ackMetadata = snapshotSinkBufferManager.generateAckMetadata(message);
                if (ackMetadata.getEntryType() == LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE) {
                    log.warn("Resend snapshot sync transfer complete ack. Sink state={}, received={}", rxState,
                            message.getMetadata().getEntryType());
                    return getLrEntryAckMsg(ackMetadata);
                }
            }

            // Drop all other invalid messages
            log.warn("Sink Manager in state {} and received message {}. Dropping Message.", rxState,
                    message.getMetadata().getEntryType());

            return null;
        }

        return processReceivedMessage(message);
    }

    private boolean isMessageFromNewSnapshotSync(LogReplication.LogReplicationEntryMsg message) {
        return ((message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START ||
            message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_MESSAGE ||
            message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END) &&
            !Objects.equals(getUUID(message.getMetadata().getSyncRequestId()), lastSnapshotSyncId));
    }

    /**
     * Process received (valid) message depending on the current rx state (LOG_ENTRY_SYNC or SNAPSHOT_SYNC)
     *
     * @param message received message
     * @return ack
     */
    private LogReplication.LogReplicationEntryMsg processReceivedMessage(LogReplication.LogReplicationEntryMsg message) {
        if (rxState.equals(RxState.LOG_ENTRY_SYNC)) {
            return logEntrySinkBufferManager.processMsgAndBuffer(message);
        } else {
            return snapshotSinkBufferManager.processMsgAndBuffer(message);
        }
    }

    private void processSnapshotSyncApplied(LogReplication.LogReplicationEntryMsg entry) {
        long lastAppliedBaseSnapshotTimestamp = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        long latestSnapshotSyncCycleId = logReplicationMetadataManager.getCurrentSnapshotSyncCycleId();
        long ackSnapshotSyncCycleId = entry.getMetadata().getSyncRequestId().getMsb() & Long.MAX_VALUE;
        // Verify this snapshot ACK corresponds to the last initialized/valid snapshot sync
        // as a previous one could have been canceled but still processed due to messages being out of order
        if ((ackSnapshotSyncCycleId == latestSnapshotSyncCycleId) &&
                (entry.getMetadata().getSnapshotTimestamp() == lastAppliedBaseSnapshotTimestamp)) {
            // Notify end of snapshot sync. This is a blocking call.
            log.info("Notify Snapshot Sync Plugin completion of snapshot sync id={}, baseSnapshot={}", ackSnapshotSyncCycleId,
                    lastAppliedBaseSnapshotTimestamp);
            log.info("Enter onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
            snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
            log.info("Exit onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
        } else {
            log.warn("SNAPSHOT_SYNC has completed for {}, but new ongoing SNAPSHOT_SYNC is {}. Id mismatch :: " +
                            "current_snapshot_cycle_id={}, ack_cycle_id={}",
                    entry.getMetadata().getSnapshotTimestamp(), lastAppliedBaseSnapshotTimestamp, latestSnapshotSyncCycleId,
                    ackSnapshotSyncCycleId);
        }
    }

    /**
     * Verify if current Snapshot Start message determines the start
     * of a valid Snapshot Sync cycle.
     *
     * @param entry received entry
     * @return true, if it is a valid snapshot start marker
     *         false, otherwise
     */
    private boolean isValidSnapshotStart(LogReplication.LogReplicationEntryMsg entry) {
        long topologyConfigId = entry.getMetadata().getTopologyConfigID();
        long messageBaseSnapshot = entry.getMetadata().getSnapshotTimestamp();
        UUID messageSnapshotId = getUUID(entry.getMetadata().getSyncRequestId());

        log.debug("Received snapshot sync start marker with request id {} on base snapshot timestamp {}",
                entry.getMetadata().getSyncRequestId(), entry.getMetadata().getSnapshotTimestamp());

        // Drop out of date messages, that have been resent
        // If no further writes have come into the log, the baseSnapshotTimestamp could be the same,
        // for this reason we should also compare based on the snapshot sync identifier
        if (messageBaseSnapshot <= baseSnapshotTimestamp && messageSnapshotId != null && messageSnapshotId.equals(lastSnapshotSyncId)) {
            log.warn("Sink Manager, state={} while received message={}. " +
                            "Dropping message with smaller snapshot timestamp than current {}",
                    rxState, entry.getMetadata(), baseSnapshotTimestamp);
            return false;
        }

        // Fails to set the baseSnapshot at the metadata store, it could be a out of date message,
        // or the current node is out of sync, ignore it.
        if (!logReplicationMetadataManager.setBaseSnapshotStart(topologyConfigId, messageBaseSnapshot)) {
            log.warn("Sink Manager in state {} and received message {}. " +
                            "Dropping Message due to failure to update the metadata store {}",
                    rxState, entry.getMetadata(), logReplicationMetadataManager);
            return false;
        }

        lastSnapshotSyncId = messageSnapshotId;
        return true;
    }

    /**
     * Process a SNAPSHOT_START message. This message will not be pushed to the buffer,
     * as it triggers a transition and resets the state.
     * If it is requesting a new snapshot with higher timestamp, transition to SNAPSHOT_SYNC state,
     * otherwise ignore the message.
     *
     * @param entry a SNAPSHOT_START message
     */
    private synchronized void processSnapshotStart(LogReplication.LogReplicationEntryMsg entry) {
        long topologyId = entry.getMetadata().getTopologyConfigID();
        long timestamp = entry.getMetadata().getSnapshotTimestamp();

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(topologyId, timestamp);

        // Update lastTransferDone with the new snapshot transfer timestamp.
        baseSnapshotTimestamp = entry.getMetadata().getSnapshotTimestamp();

        // Setup buffer manager.
        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastSnapshotTransferredSequenceNumber(), this);

        // Set state in SNAPSHOT_SYNC state.
        rxState = RxState.SNAPSHOT_SYNC;
        log.info("Sink manager entry {} state, snapshot start with {}",
                rxState, TextFormat.shortDebugString(entry.getMetadata()));
    }

    /**
     * Given that snapshot sync apply phase has finished, set the corresponding
     * metadata and signal external plugin on completion of snapshot sync, so
     * checkpoint/trim process can be resumed.
     */
    private void completeSnapshotApply(LogReplication.LogReplicationEntryMsg entry) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    logReplicationMetadataManager.setSnapshotAppliedComplete(entry);
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to set SNAPSHOT_SYNC as completed.", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to set SNAPSHOT_SYNC as completed.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }

        processSnapshotSyncApplied(entry);

        // TODO V2: revisit this when increasing the number of threads in logReplicationServer. (fix in PR 3750)
        // snapshot_Start and completeSnapshotApply is executed by different threads, and they race on updating rxState.
        // Consider this scenario: Thread1 is working on a snapshot apply with baseSnapshotTimestamp T1 and comes here
        // to update the in-memory states.
        // At the same time thread2 receives a snapshot_start msg and updates the baseSnapshotTimestamp to T2 and
        // updates rxState to Snapshot_Sync.
        // Thread1 updates rxState to Log_entry_sync and exits.
        // Now, the incoming snapshot messages will be dropped as the rxState = Log_entry_sync.
        // checking baseSnapshotTimestamp before updating rxState will resolve this race condition.
        synchronized (this) {
            if (entry.getMetadata().getSnapshotTimestamp() < baseSnapshotTimestamp) {
                log.warn("Not transitioning to Log_Entry sync, applied snapshotTs {} is before the current " +
                        "baseSnapshotTs {}", baseSnapshotTimestamp, entry.getMetadata().getSnapshotTimestamp());
                return;
            }

            rxState = RxState.LOG_ENTRY_SYNC;

            // Create the Sink Buffer Manager with the last processed timestamp as the snapshot timestamp (log entry
            // batch processed timestamp is already updated to the snapshot timestamp
            logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                    logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp(), this);
            logEntryWriter.reset(entry.getMetadata().getSnapshotTimestamp(), entry.getMetadata().getSnapshotTimestamp());

            log.info("Snapshot apply complete, sync_id={}, snapshot={}, state={}", entry.getMetadata().getSyncRequestId(),
                    entry.getMetadata().getSnapshotTimestamp(), rxState);
        }

    }

    /**
     * Process transferred snapshot sync messages
     *
     * @param entry received entry
     */
    private void processSnapshotMessage(LogReplication.LogReplicationEntryMsg entry) {
        switch (entry.getMetadata().getEntryType()) {
            case SNAPSHOT_MESSAGE:
                snapshotWriter.apply(entry);
                break;
            case SNAPSHOT_END:
                if (snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.APPLY_PHASE) {
                    completeSnapshotTransfer(entry);
                    startSnapshotApplyAsync(entry);
                }
                break;
            default:
                log.warn("Message type {} should not be applied during snapshot sync.", entry.getMetadata().getEntryType());
                break;
        }
    }

    private synchronized void startSnapshotApplyAsync(LogReplication.LogReplicationEntryMsg entry) {
        if (!ongoingApply.get()) {
            ongoingApply.set(true);
            applyExecutor.submit(() -> startSnapshotApply(entry));
        }
    }

    private synchronized void startSnapshotApply(LogReplication.LogReplicationEntryMsg entry) {
        log.debug("Entry Start Snapshot Sync Apply, id={}", entry.getMetadata().getSyncRequestId());

        if (waitMsBeforeSnapshotApply > 0) {
            log.info("Waiting for {} ms before starting Snapshot Apply", waitMsBeforeSnapshotApply);
            try {
                TimeUnit.MILLISECONDS.sleep(waitMsBeforeSnapshotApply);
            } catch (InterruptedException e) {
                log.warn("Snapshot Apply Wait Interrupted.  Continuing Snapshot Apply");
            }
        }

        // set data_consistent as false
        setDataConsistentWithRetry(false);
        
        // Sync with registry after transfer phase to capture local updates, as transfer phase could
        // take a relatively long time.
        config.syncWithRegistry();
        snapshotWriter.clearLocalStreams();
        snapshotWriter.startSnapshotSyncApply();
        completeSnapshotApply(entry);
        ongoingApply.set(false);
        log.debug("Exit Start Snapshot Sync Apply, id={}", entry.getMetadata().getSyncRequestId());
    }

    private void completeSnapshotTransfer(LogReplication.LogReplicationEntryMsg message) {
        // Update metadata, indicating snapshot transfer completeness
        logReplicationMetadataManager.setLastSnapshotTransferCompleteTimestamp(topologyConfigId,
                message.getMetadata().getSnapshotTimestamp());
    }

    /**
     * While processing an in order message, the buffer will callback and process the message
     * @param message
     * @return true if msg was processed else false.
     */
    public boolean processMessage(LogReplication.LogReplicationEntryMsg message) {
        log.trace("Received dataMessage by Sink Manager. Total [{}]", rxMessageCounter);

        switch (rxState) {
            case LOG_ENTRY_SYNC:
                return logEntryWriter.apply(message);

            case SNAPSHOT_SYNC:
                processSnapshotMessage(message);
                return true;

            default:
                log.error("Wrong state {}.", rxState);
                return false;
        }
    }

    /**
     * Verify if the message is the correct type for the current state.
     *
     * @param message received entry
     * @return true, if received message is valid for the current sink state
     *         false, otherwise
     */
    private boolean receivedValidMessage(LogReplication.LogReplicationEntryMsg message) {
        return rxState == RxState.SNAPSHOT_SYNC && (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_MESSAGE
                || message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END)
                || rxState == RxState.LOG_ENTRY_SYNC && message.getMetadata().getEntryType() == LogReplicationEntryType.LOG_ENTRY_MESSAGE;
    }

    /**
     * Update the topology config id
     *
     * @param topologyConfigId
     */
    public void updateTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    /**
     * When there is a cluster role change, the Sink Manager needs to do the following:
     *
     * 1. Reset snapshotWriter and logEntryWriter state
     * 2. Reset buffer logEntryBuffer state.
     *
     * */
    public void reset() {
        long lastAppliedSnapshotTimestamp = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        long lastProcessedLogEntryTimestamp = logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp();
        log.debug("Reset Sink Manager, lastAppliedSnapshotTs={}, lastProcessedLogEntryTs={}", lastAppliedSnapshotTimestamp,
                lastProcessedLogEntryTimestamp);
        snapshotWriter.reset(topologyConfigId, lastAppliedSnapshotTimestamp);
        logEntryWriter.reset(lastAppliedSnapshotTimestamp, lastProcessedLogEntryTimestamp);
        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                lastProcessedLogEntryTimestamp, this);
    }

    public void shutdown() {
        this.runtime.shutdown();
        this.applyExecutor.shutdownNow();
    }

    /**
     * Resume Snapshot Sync Apply
     *
     * In the event of restarts, a Snapshot Sync which had finished transfer can resume the apply stage.
     */
    public void resumeSnapshotApply() {
        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(topologyConfigId, logReplicationMetadataManager.getLastStartedSnapshotTimestamp());
        long snapshotTransferTs = logReplicationMetadataManager.getLastTransferredSnapshotTimestamp();
        UUID snapshotSyncId = new UUID(logReplicationMetadataManager.getCurrentSnapshotSyncCycleId(), Long.MAX_VALUE);
        log.info("Resume Snapshot Sync Apply, snapshot_transfer_ts={}, id={}", snapshotTransferTs, snapshotSyncId);
        // Construct Log Replication Entry message used to complete the Snapshot Sync with info in the metadata manager
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
                .setTopologyConfigID(logReplicationMetadataManager.getTopologyConfigId())
                .setTimestamp(-1L)
                .setSnapshotTimestamp(snapshotTransferTs)
                .setSyncRequestId(getUuidMsg(snapshotSyncId)).build();
        startSnapshotApplyAsync(getLrEntryAckMsg(metadata));
    }

    /**
     * Stop any functions on Sink Manager when leadership is lost
     */
    public void stopOnLeadershipLoss() {
        // If current sink/standby is in TRANSFER phase, trigger end of snapshot sync (unfreeze checkpoint) as we
        // don't know when snapshot sync might be started again.
        // If in APPLY phase do not unfreeze or shadow streams could be lost. This change was done near the release
        // date we don't know if we would be able to recover from this (test this scenario)
        // TODO: check if we'd recover from trim in shadow streams by the protocol itself
        if (rxState == RxState.SNAPSHOT_SYNC) {
            if (snapshotWriter.getPhase() == StreamsSnapshotWriter.Phase.TRANSFER_PHASE) {
                log.warn("Leadership lost while in TRANSFER phase. Trigger " +
                    "snapshot sync plugin end, to avoid effects of" +
                    "delayed restarts of snapshot sync.");
                log.info("Run onSnapshotSyncEnd :: {}",
                    snapshotSyncPlugin.getClass().getSimpleName());
                snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
                log.info("Completed onSnapshotSyncEnd :: {}",
                    snapshotSyncPlugin.getClass().getSimpleName());
            } else {
                log.warn("Leadership lost while in APPLY phase. Note that snapshot sync end plugin might not " +
                    "have been ran.");
            }
        } else {
            log.info("Leadership lost while in Log Entry Sync State");
        }
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}
