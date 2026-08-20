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
import java.util.concurrent.ScheduledExecutorService;
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

    // How often to check whether the current snapshot sync attempt has gone silent (see
    // checkSnapshotSyncLiveness()). Deliberately smaller than the self-unfreeze timeout itself so
    // the check granularity doesn't materially delay the reaction once the threshold is crossed.
    private static final long SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS = 5000;

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

    // Set while a received snapshot-sync message is actively being written (e.g. blocked on fsync),
    // so the source can distinguish "sink is alive but slow" from "sink is unresponsive" instead of
    // relying solely on a fixed ack timeout.
    private final AtomicBoolean processingSnapshotWrite = new AtomicBoolean(false);

    // Timestamp of the most recent transition of processingSnapshotWrite/ongoingApply, in either
    // direction. Lets isProcessingSnapshotSync() report "busy" for a short window even between two
    // discrete write bursts, rather than being a point-in-time snapshot that can under-report
    // busyness purely due to when it happens to be sampled.
    private volatile long lastProcessingActivityTimeMs = System.currentTimeMillis();

    // Timestamp of the most recent snapshot-sync-related message received, used to detect that the
    // source has gone silent on the current attempt (e.g. it canceled and is backing off before
    // retrying, or the connection dropped) so local checkpointing can be unfrozen proactively instead
    // of waiting on a message that may not arrive for a while, if at all.
    private volatile long lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();

    // Guards against calling onSnapshotSyncEnd() on every liveness-check tick once the self-timeout
    // has fired for the current idle period; cleared as soon as new activity is observed.
    private final AtomicBoolean selfUnfrozeForCurrentIdlePeriod = new AtomicBoolean(false);

    // Timestamp ongoingApply last transitioned to true, used by checkForStuckApply() to detect a
    // genuine hang (as opposed to startSnapshotApply() failing fast, which its own try/finally
    // already handles -- see that method's Javadoc). There is no safe way in Java to forcibly
    // reclaim a thread stuck in a non-interruptible call, so this is deliberately detection/alerting
    // only: it can't unstick the hung thread or free up applyExecutor's single worker, but it turns
    // an otherwise totally silent, permanent deadlock into a loud, operator-visible one.
    private volatile long applyStartTimeMs = 0;

    // Guards against re-logging the stuck-apply alert on every watchdog tick once it's fired once for
    // the current apply attempt; cleared as soon as a new apply attempt starts.
    private final AtomicBoolean loggedStuckApplyForCurrentAttempt = new AtomicBoolean(false);

    private final ScheduledExecutorService snapshotSyncLivenessExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("snapshotSyncLivenessExecutor")
                    .build());

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
                .cacheDisabled(true)
                .maxWriteSize(context.getMaxWriteSize())
                .build())
                .parseConfigurationString(localCorfuEndpoint).connect();
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
                .cacheDisabled(true)
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

        this.snapshotSyncLivenessExecutor.scheduleWithFixedDelay(this::checkSnapshotSyncLiveness,
                SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS, SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        this.snapshotSyncLivenessExecutor.scheduleWithFixedDelay(this::checkForStuckApply,
                SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS, SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

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
                // processSnapshotStart() resets the liveness clock and calls the snapshot sync
                // plugin's start (freeze) callback itself, both inside the same synchronized(this)
                // boundary that checkSnapshotSyncLiveness() uses for its unfreeze decision -- see
                // that method's Javadoc for why this matters (a stale unfreeze decision for a prior,
                // now-abandoned attempt must not be able to land after this attempt has re-frozen).
                processSnapshotStart(message);
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
        }
        long lastProcessedSeqBefore = snapshotSinkBufferManager.lastProcessedSeq;
        processingSnapshotWrite.set(true);
        lastProcessingActivityTimeMs = System.currentTimeMillis();
        try {
            LogReplication.LogReplicationEntryMsg ack = snapshotSinkBufferManager.processMsgAndBuffer(message);
            // Only genuine forward progress counts as liveness activity -- shouldAck() acks on a
            // time/count cadence independent of whether this specific message advanced anything, so
            // a duplicate or out-of-order message can still produce a non-null ack. Gating on
            // lastProcessedSeq actually changing is what distinguishes "the sink made progress" from
            // "the sink said something", which matters because the source may be blindly resending an
            // unacked entry indefinitely (SenderBufferManager.resend() has no retry cap) -- without
            // this, that alone would keep resetting checkSnapshotSyncLiveness()'s idle timer forever.
            if (snapshotSinkBufferManager.lastProcessedSeq != lastProcessedSeqBefore) {
                lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();
                selfUnfrozeForCurrentIdlePeriod.set(false);
            }
            return ack;
        } finally {
            processingSnapshotWrite.set(false);
            lastProcessingActivityTimeMs = System.currentTimeMillis();
        }
    }

    /**
     * Whether the sink is currently actively working on the in-progress snapshot sync (writing an
     * incoming batch, or applying a completed transfer), as opposed to merely having one open with
     * nothing happening. Used by the source to tell a busy-but-alive sink apart from a stalled one.
     *
     * Reports "busy" for a short window after the last write/apply transition too (see
     * PROCESSING_ACTIVITY_WINDOW_MS), not just while a flag happens to be true at the exact instant
     * this is called -- otherwise this is a point-in-time snapshot that can under-report continuous
     * load purely because it was sampled in the gap between two discrete write bursts.
     */
    public boolean isProcessingSnapshotSync() {
        return isWriteOrApplyInFlight()
                || (System.currentTimeMillis() - lastProcessingActivityTimeMs) < LogReplicationConfig.PROCESSING_ACTIVITY_WINDOW_MS;
    }

    /**
     * Whether a write or apply is in flight at this exact instant, with no smoothing window applied.
     * Unlike isProcessingSnapshotSync(), this does NOT treat a message merely having been received
     * recently as "busy" -- only used where that distinction matters (checkSnapshotSyncLiveness()):
     * isProcessingSnapshotSync()'s windowed component exists to bridge the ~50ms gap between two
     * discrete write bursts for the source's short (DEFAULT_TIMEOUT_MS-scale) busy poll, and every
     * call into processReceivedMessage() re-arms it regardless of whether that call advanced anything
     * -- including a duplicate or out-of-order message. At checkSnapshotSyncLiveness()'s much longer
     * (SINK_SELF_UNFREEZE_TIMEOUT_MS-scale) timeout, that same windowing lets a steady drip of
     * non-advancing messages (spaced closer than PROCESSING_ACTIVITY_WINDOW_MS) look perpetually busy
     * forever, defeating self-unfreeze exactly in the scenario it exists to handle.
     */
    private boolean isWriteOrApplyInFlight() {
        return processingSnapshotWrite.get() || ongoingApply.get();
    }

    /**
     * Runs periodically while a snapshot sync is open. If the source has gone silent on the current
     * TRANSFER-phase attempt (no message at all, not even one indicating it's still working) for
     * longer than the calibrated self-unfreeze timeout, unfreeze local checkpointing proactively
     * instead of waiting on a retry (or a connection recovery) that may not come for a while.
     *
     * Deliberately restricted to the TRANSFER phase: unfreezing during APPLY risks the checkpointer
     * trimming shadow-stream data the apply still needs, mirroring the same restriction already
     * applied in stopOnLeadershipLoss().
     *
     * The read-idle-then-CAS-then-call sequence below is deliberately inside a synchronized(this)
     * block sharing the same monitor as processSnapshotStart(). Without that, a fresh SNAPSHOT_START
     * for a new attempt could land -- resetting lastSnapshotSyncActivityTimeMs and re-freezing --
     * in the gap between this method reading a stale idleMs (for the OLD, now-abandoned attempt) and
     * actually calling onSnapshotSyncEnd(), incorrectly lifting the new attempt's freeze instead of
     * the old one's. That window is narrow but not negligible: it's most likely to actually be hit
     * early in a backoff sequence (e.g. ~16-32s backoff), where the gap between this timeout firing
     * and the next SNAPSHOT_START arriving is only a few seconds rather than the tens-of-seconds
     * margin the steady-state (60s-capped backoff) case has. Synchronizing on the same monitor makes
     * the two mutually exclusive: whichever happens first (the stale unfreeze decision completing, or
     * the new attempt's reset) fully completes before the other can begin.
     *
     * The guard clause is deliberately evaluated twice: once unsynchronized (fast path) and once
     * again inside the synchronized block (the one that actually matters for correctness -- classic
     * double-checked locking). This matters specifically because startSnapshotApply() (and
     * processSnapshotStart()) also hold this same monitor: if apply ever genuinely hangs (as opposed
     * to failing fast, which startSnapshotApply()'s try/finally already handles) rather than just
     * running long, a version of this method that unconditionally entered synchronized(this) first
     * would block this scheduled check forever too, right along with everything else contending for
     * the monitor -- entirely defeating the purpose of a periodic liveness check. The unsynchronized
     * guard clause reads ongoingApply (via isWriteOrApplyInFlight(), a lock-free AtomicBoolean) and
     * snapshotWriter.getPhase() (a plain, unsynchronized field read on a different object) -- both
     * readable without contending for this monitor at all, and ongoingApply is set true before
     * startSnapshotApply() is even submitted and stays true for that call's entire duration
     * (including any hang), so this fast path reliably and correctly bails out during any hang,
     * regardless of whether the hang happens before or after the phase actually flips to APPLY_PHASE.
     *
     * Deliberately checks isWriteOrApplyInFlight(), not isProcessingSnapshotSync(): the latter's
     * PROCESSING_ACTIVITY_WINDOW_MS smoothing is re-armed by every call into processReceivedMessage(),
     * including a non-advancing duplicate or out-of-order message, which would let a steady drip of
     * such messages (spaced closer than that window) mask the exact silent-source scenario this
     * method exists to detect. lastSnapshotSyncActivityTimeMs below is the correctly progress-gated
     * signal for that; isWriteOrApplyInFlight() only guards against firing mid-write.
     */
    private void checkSnapshotSyncLiveness() {
        try {
            if (rxState != RxState.SNAPSHOT_SYNC
                    || snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.TRANSFER_PHASE
                    || isWriteOrApplyInFlight()) {
                selfUnfrozeForCurrentIdlePeriod.set(false);
                return;
            }

            synchronized (this) {
                // Re-verify: the fast, unsynchronized check above could be stale by the time we get
                // here (e.g. a fresh SNAPSHOT_START landed in between). Everything from here down is
                // atomic with processSnapshotStart()'s reset.
                if (rxState != RxState.SNAPSHOT_SYNC
                        || snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.TRANSFER_PHASE
                        || isWriteOrApplyInFlight()) {
                    selfUnfrozeForCurrentIdlePeriod.set(false);
                    return;
                }

                long idleMs = System.currentTimeMillis() - lastSnapshotSyncActivityTimeMs;
                if (idleMs > LogReplicationConfig.SINK_SELF_UNFREEZE_TIMEOUT_MS
                        && selfUnfrozeForCurrentIdlePeriod.compareAndSet(false, true)) {
                    log.warn("No activity for snapshot sync {} for {} ms while in TRANSFER phase; unfreezing local " +
                            "checkpointing proactively instead of waiting on the source.", lastSnapshotSyncId, idleMs);
                    log.info("Enter onSnapshotSyncEnd (self-timeout) :: {}", snapshotSyncPlugin.getClass().getSimpleName());
                    snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
                    log.info("Exit onSnapshotSyncEnd (self-timeout) :: {}", snapshotSyncPlugin.getClass().getSimpleName());
                }
            }
        } catch (Throwable t) {
            log.error("Error while checking snapshot sync liveness.", t);
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
        lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();
        selfUnfrozeForCurrentIdlePeriod.set(false);
        log.info("Sink manager entry {} state, snapshot start with {}",
                rxState, TextFormat.shortDebugString(entry.getMetadata()));

        // The SnapshotPlugin is called when LR is ready to start a snapshot sync, so the system can
        // prepare for the full sync -- typically, to stop checkpoint/trim during the snapshot sync to
        // prevent data loss from shadow tables (temporal non-checkpointed streams). This is a
        // blocking call. It's deliberately inside this synchronized method (rather than in the
        // receive() caller, as it was previously) so it's atomic with respect to
        // checkSnapshotSyncLiveness()'s unfreeze decision, which synchronizes on the same monitor:
        // a stale unfreeze decision for a prior, now-abandoned attempt can no longer land after this
        // freshly-started attempt has already re-frozen, and vice versa.
        log.info("Enter onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
        snapshotSyncPlugin.onSnapshotSyncStart(runtime);
        log.info("Exit onSnapshotSyncStart :: {}", snapshotSyncPlugin.getClass().getSimpleName());
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
            lastProcessingActivityTimeMs = System.currentTimeMillis();
            applyStartTimeMs = System.currentTimeMillis();
            loggedStuckApplyForCurrentAttempt.set(false);
            applyExecutor.submit(() -> startSnapshotApply(entry));
        }
    }

    /**
     * Runs periodically while a snapshot sync is open. Detects a genuinely hung apply -- as opposed
     * to one that fails fast, which startSnapshotApply()'s try/finally already handles -- and logs a
     * loud, explicit alert. This is deliberately detection/alerting only: there is no safe way in
     * Java to forcibly reclaim a thread stuck in a non-interruptible call (e.g. blocked disk I/O with
     * no internal timeout, or a genuine deadlock), and applyExecutor has exactly one worker thread,
     * so even if ongoingApply were forced back to false here, a subsequent apply attempt would just
     * queue behind the still-running hung task forever rather than actually making progress. Recovery
     * from this specific case requires an operator (or automation watching for this log line)
     * restarting the sink process. Turning an otherwise totally silent, permanent deadlock into a
     * loud, operator-visible one is the achievable improvement here.
     */
    private void checkForStuckApply() {
        try {
            if (!ongoingApply.get()) {
                loggedStuckApplyForCurrentAttempt.set(false);
                return;
            }

            long applyElapsedMs = System.currentTimeMillis() - applyStartTimeMs;
            if (applyElapsedMs > LogReplicationConfig.SNAPSHOT_SYNC_APPLY_MAX_WAIT_MS
                    && loggedStuckApplyForCurrentAttempt.compareAndSet(false, true)) {
                log.error("Snapshot sync apply has been ongoing for {} ms with no completion -- this may be a " +
                        "genuine hang (e.g. blocked I/O or a deadlock) that this process cannot recover from on " +
                        "its own. Manual intervention (restarting this sink) is likely required if this " +
                        "persists.", applyElapsedMs);
            }
        } catch (Throwable t) {
            log.error("Error while checking for a stuck snapshot sync apply.", t);
        }
    }

    private synchronized void startSnapshotApply(LogReplication.LogReplicationEntryMsg entry) {
        log.debug("Entry Start Snapshot Sync Apply, id={}", entry.getMetadata().getSyncRequestId());

        // Guards the whole apply attempt: previously, any exception thrown below (most notably a
        // TrimmedException -- e.g. an external, LR-unaware safety valve like the checkpointer's own
        // freeze-token auto-expiry running a checkpoint+trim concurrently and removing shadow-stream
        // data this apply still needed) would propagate out of this Runnable uncaught. Nobody calls
        // .get() on the Future returned by applyExecutor.submit(), so the exception was silently
        // swallowed, and because ongoingApply.set(false) below never ran, ongoingApply stayed true
        // permanently -- which also permanently blocks every future attempt, since receive() drops
        // any new snapshot sync's messages (including its SNAPSHOT_START) while ongoingApply is true.
        // The result was a silent, total, unrecoverable deadlock of the whole replication session,
        // with the source's WaitSnapshotApplyState polling forever for a completion that could never
        // come. This attempt's transferred data is unrecoverable in that scenario regardless (the
        // shadow streams it depended on are gone) -- the only correct recovery is to abandon this
        // attempt cleanly and let a fresh full transfer happen, which is exactly what resetting
        // ongoingApply enables: it un-gates receive() so the next SNAPSHOT_START is accepted instead
        // of dropped, and processSnapshotStart() unconditionally resets everything else needed.
        try {
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
            log.debug("Exit Start Snapshot Sync Apply, id={}", entry.getMetadata().getSyncRequestId());
        } catch (Throwable t) {
            log.error("Snapshot sync apply failed for id={}; abandoning this attempt -- its transferred " +
                    "data is unrecoverable, a fresh full snapshot sync will be needed.",
                    entry.getMetadata().getSyncRequestId(), t);
        } finally {
            ongoingApply.set(false);
            lastProcessingActivityTimeMs = System.currentTimeMillis();
        }
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
     * Test-only seam to inject a substitute snapshot writer (e.g. one that throws on demand), so
     * failure paths that are otherwise only reachable via an actual concurrent trim (see
     * startSnapshotApply()'s exception handling) can be exercised deterministically.
     */
    @VisibleForTesting
    public void setSnapshotWriter(StreamsSnapshotWriter snapshotWriter) {
        this.snapshotWriter = snapshotWriter;
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
        this.snapshotSyncLivenessExecutor.shutdownNow();
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
