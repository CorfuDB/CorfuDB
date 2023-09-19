package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_RECV_Q_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

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

    @Getter
    private final LogReplicationSession session;

    // Duration in milliseconds after which an ACK is sent back to the sender
    // if the message count is not reached before
    private int ackCycleTime = DEFAULT_ACK_CNT;

    // Number of messages received before sending a summarized ACK
    private int ackCycleCnt;

    private int bufferSize;

    private final CorfuRuntime runtime;

    private LogReplicationContext replicationContext;

    private LogEntrySinkBufferManager logEntrySinkBufferManager;
    private SnapshotSinkBufferManager snapshotSinkBufferManager;

    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;

    @Getter
    private LogReplicationMetadataManager metadataManager;

    private RxState rxState;

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

    private ExecutorService applyExecutor;

    @Getter
    private final AtomicBoolean ongoingApply = new AtomicBoolean(false);

    @Getter
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Constructor Sink Manager
     *
     * @param metadataManager manages log replication session's metadata
     * @param session log replication session unique identifier
     * @param replicationContext log replication context
     */
    public LogReplicationSinkManager(LogReplicationMetadataManager metadataManager, LogReplicationSession session,
                                     LogReplicationContext replicationContext) {

        this.replicationContext = replicationContext;
        this.runtime = replicationContext.getCorfuRuntime();
        this.topologyConfigId = replicationContext.getTopologyConfigId();
        this.session = session;
        this.metadataManager = metadataManager;

        init();
    }

    /**
     * Initialize common parameters
     */
    private void init() {
        // When the server is up, it will be at LOG_ENTRY_SYNC state by default.
        // The sender will query receiver's status and decide what type of replication to start with.
        // It will transit to SNAPSHOT_SYNC state if it received a SNAPSHOT_START message from the sender.
        this.rxState = RxState.LOG_ENTRY_SYNC;

        this.applyExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("snapshotSyncApplyExecutor-" + session.hashCode())
                        .build());

        if (session.getSubscriber().getModel().equals(LogReplication.ReplicationModel.ROUTING_QUEUES)) {
            insertQInRegistryTable();
        }
        initWriterAndBufferMgr();
    }

    private void setDataConsistentWithRetry(boolean isDataConsistent) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                     metadataManager.setDataConsistentOnSink(isDataConsistent, session);
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
        snapshotWriter = new StreamsSnapshotWriter(runtime, metadataManager, session, replicationContext);
        logEntryWriter = new LogEntryWriter(metadataManager, session, replicationContext);

        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                metadataManager.getReplicationMetadata(session).getLastLogEntryBatchProcessed(), this);
    }

    /**
     * For routing table model, the replicated queue, which is present only on SINK side, may not be open by the time
     * LR starts the replication. As we want the stream to be checkpointed, add an entry to the registry table.
     * No need to open the queue.
     */
    private void insertQInRegistryTable() {
        TableRegistry tableRegistry = runtime.getTableRegistry();
        String replicatedQName =
            REPLICATED_RECV_Q_PREFIX + session.getSourceClusterId() + "_" + session.getSubscriber().getClientName();
        if (!tableRegistry.getRegistryTable().containsKey(replicatedQName)) {
            try {
                TableOptions tableOptions = TableOptions.builder()
                        .schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                                .addStreamTag(REPLICATED_QUEUE_TAG)
                                .build())
                        .persistentDataPath(Paths.get("/nonconfig/logReplication/RoutingQModel/", replicatedQName))
                        .build();
                tableRegistry.registerTable(CORFU_SYSTEM_NAMESPACE, replicatedQName, Queue.CorfuGuidMsg.class,
                        Queue.RoutingTableEntryMsg.class, Queue.CorfuQueueMetadataMsg.class,
                        tableOptions);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ISnapshotSyncPlugin getOnSnapshotSyncPlugin() {
        LogReplicationPluginConfig config = replicationContext.getPluginConfig();
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
        // TODO V2: This file path can be added to the ReplicationContext and be fetched from there
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
            log.warn("Config file {} does not exist. Using default configs", CONFIG_FILE);
        } catch (IOException e) {
            log.error("IO Exception when reading config file", e);
        }
        log.info("Sink Manager Buffer config queue size {} ackCycleCnt {} ackCycleTime {}", bufferSize, ackCycleCnt,
            ackCycleTime);
    }

    /**
     * Receive a message from the sender.
     *
     * @param message
     * @return
     */
    @Override
    public synchronized LogReplicationEntryMsg receive(LogReplicationEntryMsg message) {
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
            // sends the SNAPSHOT_END marker again, but the receiver has already transitioned to
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

    /**
     * Process received (valid) message depending on the current rx state (LOG_ENTRY_SYNC or SNAPSHOT_SYNC)
     *
     * @param message received message
     * @return ack
     */
    private LogReplicationEntryMsg processReceivedMessage(LogReplicationEntryMsg message) {
        if (rxState.equals(RxState.LOG_ENTRY_SYNC)) {
            return logEntrySinkBufferManager.processMsgAndBuffer(message);
        } else {
            return snapshotSinkBufferManager.processMsgAndBuffer(message);
        }
    }

    private void processSnapshotSyncApplied(LogReplicationEntryMsg entry) {
        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);
        long lastAppliedBaseSnapshotTimestamp = metadata.getLastSnapshotApplied();
        UuidMsg latestSnapshotSyncCycleId = metadata.getCurrentSnapshotCycleId();
        UuidMsg ackSnapshotSyncCycleId = entry.getMetadata().getSyncRequestId();

        // Verify this snapshot ACK corresponds to the last initialized/valid snapshot sync
        // as a previous one could have been canceled but still processed due to messages being out of order
        if (Objects.equals(latestSnapshotSyncCycleId, ackSnapshotSyncCycleId) &&
            (entry.getMetadata().getSnapshotTimestamp() == lastAppliedBaseSnapshotTimestamp)) {
            // Notify end of snapshot sync. This is a blocking call.
            log.info("Notify Snapshot Sync Plugin completion of snapshot sync id={}, baseSnapshot={}", ackSnapshotSyncCycleId,
                lastAppliedBaseSnapshotTimestamp);
            log.info("Enter onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
            snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
            log.info("Exit onSnapshotSyncEnd :: {}", snapshotSyncPlugin.getClass().getSimpleName());
        } else {
            log.warn("SNAPSHOT_SYNC has completed for {}, but new ongoing SNAPSHOT_SYNC is {}. Id mismatch :: " +
                    "current_snapshot_cycle_id={}, ack_cycle_id={}", entry.getMetadata().getSnapshotTimestamp(),
                    lastAppliedBaseSnapshotTimestamp, latestSnapshotSyncCycleId, ackSnapshotSyncCycleId);
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
    private boolean isValidSnapshotStart(LogReplicationEntryMsg entry) {
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

        // Fails to set the baseSnapshot at the metadata store, it could be an out of date message,
        // or the current node is out of sync, ignore it.
       if (!metadataManager.setBaseSnapshotStart(session, topologyConfigId, messageBaseSnapshot)) {
          log.warn("Sink Manager in state {} and received message {}. " +
                           "Dropping message due to failure to update the metadata store.", rxState, entry.getMetadata());
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
    private synchronized void processSnapshotStart(LogReplicationEntryMsg entry) {
        baseSnapshotTimestamp = entry.getMetadata().getSnapshotTimestamp();

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(entry.getMetadata().getTopologyConfigID(), baseSnapshotTimestamp);

        // TODO V2: If the sink crashes in the middle of a snapshot transfer cycle, a SNAPSHOT_START message
        //  won't come and the buffer will not get created, resulting in an NPE
        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                metadataManager.getReplicationMetadata(session).getLastSnapshotTransferredSeqNumber(), this);
        rxState = RxState.SNAPSHOT_SYNC;

        log.info("Sink manager entry {} state, snapshot start with {}", rxState,
                TextFormat.shortDebugString(entry.getMetadata()));
    }

    /**
     * Given that snapshot sync apply phase has finished, set the corresponding
     * metadata and signal external plugin on completion of snapshot sync, so
     * checkpoint/trim process can be resumed.
     */
    private void completeSnapshotApply(LogReplicationEntryMsg entry) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    metadataManager.setSnapshotAppliedComplete(entry, session);
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
                    metadataManager.getReplicationMetadata(session)
                            .getLastLogEntryBatchProcessed(), this);
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
    private void processSnapshotMessage(LogReplicationEntryMsg entry) {
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

    private synchronized void startSnapshotApplyAsync(LogReplicationEntryMsg entry) {
        if (!ongoingApply.get()) {
            ongoingApply.set(true);
            applyExecutor.submit(() -> {
                try {
                    startSnapshotApply(entry);
                } catch (Exception e) {
                    log.error("Error while attempting to start snapshot apply.", e);
                    ongoingApply.set(false);
                }
            });
        }
    }

    private synchronized void startSnapshotApply(LogReplicationEntryMsg entry) {
        log.info("Start snapshot sync apply, id={}", entry.getMetadata().getSyncRequestId());
        setDataConsistentWithRetry(false);

        // Sync with registry after transfer phase to capture local updates, as transfer phase could
        // take a relatively long time.
        replicationContext.refreshConfig(session, true);
        snapshotWriter.clearLocalStreams();
        snapshotWriter.startSnapshotSyncApply();
        completeSnapshotApply(entry);
        ongoingApply.set(false);
        log.debug("Exit start snapshot sync apply, id={}", entry.getMetadata().getSyncRequestId());
    }

    private void completeSnapshotTransfer(LogReplicationEntryMsg message) {
        // Update metadata, indicating snapshot transfer completeness
        metadataManager.setLastSnapshotTransferCompleteTimestamp(session, topologyConfigId,
                message.getMetadata().getSnapshotTimestamp());
    }

    /**
     * While processing an in order message, the buffer will callback and process the message
     * @param message
     * @return true if msg was processed else false.
     */
    public boolean processMessage(LogReplicationEntryMsg message) {
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
    private boolean receivedValidMessage(LogReplicationEntryMsg message) {
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
        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);
        log.debug("Reset sink manager, lastAppliedSnapshotTs={}, lastProcessedLogEntryTs={}", metadata.getLastSnapshotApplied(),
                metadata.getLastLogEntryBatchProcessed());
        snapshotWriter.reset(topologyConfigId, metadata.getLastSnapshotApplied());
        logEntryWriter.reset(metadata.getLastSnapshotApplied(), metadata.getLastLogEntryBatchProcessed());
        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                metadata.getLastLogEntryBatchProcessed(), this);
    }

    public void shutdown() {
        this.applyExecutor.shutdownNow();
        isShutdown.set(true);
    }

    /**
     * Resume snapshot sync apply phase
     *
     * In the event of node restarts/failures, a snapshot sync which was fully transferred
     * can resume from the apply stage.
     */
    private void resumeSnapshotApply() {

        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(topologyConfigId, metadata.getLastSnapshotStarted());
        long snapshotTransferTs = metadata.getLastSnapshotTransferred();
        UUID snapshotSyncId = new UUID(metadata.getCurrentSnapshotCycleId().getMsb(), metadata.getCurrentSnapshotCycleId().getLsb());
        log.info("Resume Snapshot Sync Apply, snapshot_transfer_ts={}, id={}", snapshotTransferTs, snapshotSyncId);

        // Construct message used to complete (ack) the snapshot sync transfer
        LogReplicationEntryMetadataMsg metadataMsg = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
                .setTopologyConfigID(topologyConfigId)
                .setTimestamp(Address.NON_ADDRESS)
                .setSnapshotTimestamp(snapshotTransferTs)
                .setSyncRequestId(metadata.getCurrentSnapshotCycleId())
                .build();
        startSnapshotApplyAsync(getLrEntryAckMsg(metadataMsg));
    }

    /**
     * Stop any functions on Sink Manager when leadership is lost
     */
    public void stopOnLeadershipLoss() {
        // If current sink is in TRANSFER phase, trigger end of snapshot sync (unfreeze checkpoint) as we
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

    public void startPendingSnapshotApply() {
        ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);

        boolean isSnapshotApplyPending = (metadata.getLastSnapshotStarted() == metadata.getLastSnapshotTransferred()) &&
                metadata.getLastSnapshotTransferred() > metadata.getLastSnapshotApplied();

        if (isSnapshotApplyPending && !ongoingApply.get()) {
            resumeSnapshotApply();
        }
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}
