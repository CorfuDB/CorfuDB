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
    // Mutated only under synchronized(this) (see processSnapshotStart(), completeSnapshotApply()),
    // but read from the snapshotSyncLivenessExecutor thread's unsynchronized fast-path guard clause
    // in checkSnapshotSyncLiveness() before that method ever takes the lock -- volatile so that read
    // has a defined happens-before relationship with the writes instead of relying entirely on the
    // fast path's own double-checked re-verification under the lock to mask staleness.
    private volatile RxState rxState;

    private LogReplicationConfig config;

    // Mutated only under synchronized(this) (see processSnapshotStart(), resumeSnapshotApply(),
    // completeSnapshotApply()), but read from the control-plane executor thread via
    // getBaseSnapshotTimestamp() when building a metadata response's processingSnapshotTimestamp
    // -- before that read, this attempt's own onEntry-style synchronization has already released
    // the lock, so there's no happens-before edge for a plain field the way there is for the
    // in-lock reads elsewhere in this class. volatile for the same reason rxState/phase are:
    // a defined happens-before relationship for the cross-thread read, not correctness-by-luck.
    private volatile long baseSnapshotTimestamp = Address.NON_ADDRESS - 1;
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
    // Package-private (not private) so tests can fast-forward it instead of sleeping through the
    // real SINK_SELF_UNFREEZE_TIMEOUT_MS (tens of seconds) to exercise checkSnapshotSyncLiveness().
    @VisibleForTesting
    volatile long lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();

    // Guards against calling onSnapshotSyncEnd() on every liveness-check tick once the self-timeout
    // has fired for the current idle period; cleared as soon as new activity is observed.
    private final AtomicBoolean selfUnfrozeForCurrentIdlePeriod = new AtomicBoolean(false);

    // True from the moment the first apply attempt for the current transfer-complete attempt is
    // submitted until that attempt's whole retry sequence is truly over -- either a genuine success
    // (completeSnapshotApply()) or resumeSnapshotApply() exhausting its retries -- as opposed to
    // ongoingApply, which is only true while one specific attempt is actively running.
    //
    // Deliberately kept true across the gaps *between* individual resume attempts (i.e. while
    // ongoingApply is momentarily false, waiting on resumeSnapshotApply()'s backoff), unlike the
    // previous design where startSnapshotApply()'s catch block unfroze on every single failed
    // attempt. That previous design was unsafe: unfreezing lets a compaction cycle both start and
    // complete, and TrimLog.invokePrefixTrim() does a *global*, address-based prefix trim (not
    // per-stream) up to whatever address the cycle captured at its own start -- a point necessarily
    // *after* this attempt's shadow-stream writes, since those all happened while frozen. Shadow
    // streams are raw streams (StreamsSnapshotWriter.getShadowStreamId()), never registered in the
    // TableRegistry, so they get no per-stream checkpoint protection -- freeze is the only thing
    // standing between them and being silently discarded as collateral of an unrelated cycle
    // completing. Unfreezing between retries therefore didn't just "give the checkpointer a safe
    // window" -- it gave the checkpointer an opportunity to destroy the very data the *next* retry
    // needed, converting even a transient, otherwise-recoverable failure into a certain one for the
    // rest of that give-up sequence. Staying frozen for the whole sequence and only unfreezing once
    // truly abandoning it (resumeSnapshotApply()'s cap, or genuine success) protects the sequence's
    // own chances while still bounding total freeze duration exactly as before.
    //
    // Consulted (in addition to the existing phase/ongoingApply checks) by checkSnapshotSyncLiveness()
    // and stopOnLeadershipLoss() so neither can unfreeze out from under an active retry sequence
    // either. Mutated only under synchronized(this); volatile for the same lock-free cross-thread
    // read reasons as baseSnapshotTimestamp/rxState/phase elsewhere in this class.
    @VisibleForTesting
    volatile boolean applyRetrySequenceActive = false;

    // Timestamp ongoingApply last transitioned to true, used by checkForStuckApply() to detect a
    // genuine hang (as opposed to startSnapshotApply() failing fast, which its own try/finally
    // already handles -- see that method's Javadoc). There is no safe way in Java to forcibly
    // reclaim a thread stuck in a non-interruptible call, so this is deliberately detection/alerting
    // only: it can't unstick the hung thread or free up applyExecutor's single worker, but it turns
    // an otherwise totally silent, permanent deadlock into a loud, operator-visible one.
    // Package-private (not private) so tests can fast-forward it instead of sleeping through the
    // real SNAPSHOT_SYNC_APPLY_MAX_WAIT_MS (30 minutes) to exercise checkForStuckApply().
    @VisibleForTesting
    volatile long applyStartTimeMs = 0;

    // Guards against re-logging the stuck-apply alert on every watchdog tick once it's fired once for
    // the current apply attempt; cleared as soon as a new apply attempt starts.
    private final AtomicBoolean loggedStuckApplyForCurrentAttempt = new AtomicBoolean(false);

    // Exponential-backoff state for resumeSnapshotApply()'s auto-retry trigger (invoked by
    // LogReplicationServer.handleMetadataRequest() on every metadata poll, ~every 2s, whenever the
    // metadata store shows a transfer-complete-but-not-applied attempt and no apply currently
    // ongoing). Without this, a permanently-doomed apply (e.g. the shadow streams it needs are
    // genuinely gone) would be retried on every single poll forever. Keyed on the specific pending
    // attempt's started-timestamp so a genuinely new attempt gets its own fresh backoff rather than
    // inheriting a stuck prior attempt's; both fields are only ever touched while holding this
    // instance's monitor (resumeSnapshotApply() is synchronized).
    // Package-private (not private), like InSnapshotSyncState's analogous consecutiveCancellations/
    // retryBackoffMs, so tests can seed/inspect the backoff state directly instead of sleeping
    // through real wall-clock backoff delays (which, summed across maxSnapshotApplyResumeRetries
    // attempts doubling up to MAX_RETRY_BACKOFF_MS, would make a test taking that path prohibitively
    // slow).
    @VisibleForTesting
    long resumeBackoffForStartedTimestamp = Address.NON_ADDRESS;
    @VisibleForTesting
    long resumeBackoffMs = 0;
    @VisibleForTesting
    long nextResumeAttemptTimeMs = 0;

    // Number of consecutive automatic resume attempts made for resumeBackoffForStartedTimestamp.
    // Reset alongside resumeBackoffMs/nextResumeAttemptTimeMs whenever a genuinely new attempt
    // (different startedTimestamp) appears. Once this reaches maxSnapshotApplyResumeRetries,
    // resumeSnapshotApply() stops retrying this specific stuck attempt -- see its Javadoc.
    @VisibleForTesting
    int resumeAttemptCount = 0;

    // Configurable cap on resumeAttemptCount; see LogReplicationConfig.
    // DEFAULT_MAX_SNAPSHOT_APPLY_RESUME_RETRIES's Javadoc for why this exists.
    @VisibleForTesting
    int maxSnapshotApplyResumeRetries = LogReplicationConfig.DEFAULT_MAX_SNAPSHOT_APPLY_RESUME_RETRIES;

    // Configurable minimum time (ms) requested of the source, once resumeSnapshotApply() gives up;
    // see LogReplicationConfig.DEFAULT_CHECKPOINTER_GRACE_PERIOD_MS's Javadoc for why this exists.
    @VisibleForTesting
    long checkpointerGracePeriodMs = LogReplicationConfig.DEFAULT_CHECKPOINTER_GRACE_PERIOD_MS;

    // True once resumeSnapshotApply() has given up on the current stuck attempt (see
    // resumeAttemptCount/maxSnapshotApplyResumeRetries above); cleared as soon as a genuinely new
    // attempt appears. Serves two purposes: guards against re-logging the alert on every
    // subsequent metadata poll, and -- via isApplyRetriesExhausted() below -- is surfaced to the
    // source in the metadata response so it can cancel and restart immediately instead of waiting
    // out the full SNAPSHOT_SYNC_APPLY_MAX_WAIT_MS bound (see that method's Javadoc). Mutated only
    // under synchronized(this) (resumeSnapshotApply()), but read from the control-plane executor
    // thread's lock-free isApplyRetriesExhausted() -- volatile for the same reason as
    // baseSnapshotTimestamp/rxState/phase elsewhere in this class: a defined happens-before
    // relationship for that cross-thread read, not correctness-by-luck.
    @VisibleForTesting
    volatile boolean loggedResumeExhaustedForCurrentAttempt = false;

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
     * Test-only constructor that skips every bit of external I/O the two constructors above
     * perform -- no CorfuRuntime.connect() (runtime is supplied, e.g. a Mockito mock), no
     * reflective plugin classloading from a jar path (snapshotSyncPlugin is supplied directly;
     * see initWriterAndBufferMgr()'s null-check) -- so this class's concurrency and state-machine
     * logic (the synchronized-monitor mutual exclusion between processSnapshotStart(),
     * resumeSnapshotApply() and checkSnapshotSyncLiveness(); the resume backoff/cap accounting;
     * the attempt-identity-scoped busy signal; self-unfreeze; stuck-apply detection) can be driven
     * directly and deterministically against test doubles. snapshotWriter/logEntryWriter/
     * logEntrySinkBufferManager are still constructed exactly as the real constructors do (they
     * have no external I/O of their own); override snapshotWriter afterward via
     * setSnapshotWriter() if a test needs to control apply-phase behavior specifically.
     */
    @VisibleForTesting
    public LogReplicationSinkManager(CorfuRuntime runtime, LogReplicationConfig config,
                                     LogReplicationMetadataManager metadataManager,
                                     ISnapshotSyncPlugin snapshotSyncPlugin) {
        this.runtime = runtime;
        this.pluginConfigFilePath = null;
        this.snapshotSyncPlugin = snapshotSyncPlugin;
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
        // of a snapshot sync. Skipped if a test constructor already supplied one -- see this
        // class's @VisibleForTesting constructor's Javadoc for why: the real plugin is loaded via
        // reflection from a jar path, which a unit test has no business exercising.
        if (snapshotSyncPlugin == null) {
            snapshotSyncPlugin = getOnSnapshotSyncPlugin();
        }

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
            maxSnapshotApplyResumeRetries = Integer.parseInt(props.getProperty("snapshot_apply_max_resume_retries",
                    Integer.toString(maxSnapshotApplyResumeRetries)));
            checkpointerGracePeriodMs = Long.parseLong(props.getProperty("snapshot_apply_checkpointer_grace_period_ms",
                    Long.toString(checkpointerGracePeriodMs)));
            reader.close();
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", CONFIG_FILE);
        } catch (IOException e) {
            log.error("IO Exception when reading config file", e);
        }
        log.info("Sink Manager Buffer config queue size {} ackCycleCnt {} ackCycleTime {} " +
                        "maxSnapshotApplyResumeRetries {} checkpointerGracePeriodMs {}",
                bufferSize, ackCycleCnt, ackCycleTime, maxSnapshotApplyResumeRetries, checkpointerGracePeriodMs);
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
     * The baseSnapshotTimestamp of whichever snapshot-sync attempt this sink is currently tracking
     * (transferring or applying) in memory -- may be stale/abandoned from the source's point of
     * view (e.g. this sink is still resuming an old apply the source has already canceled and
     * moved past). Used, together with isProcessingSnapshotSync(), to let the source tell "the
     * sink is busy on the attempt I'm asking about" apart from "the sink is busy on something
     * else entirely" -- see processingSnapshotTimestamp's Javadoc in the .proto for why that
     * distinction matters.
     */
    public long getBaseSnapshotTimestamp() {
        return baseSnapshotTimestamp;
    }

    /**
     * Whether resumeSnapshotApply() has given up automatically retrying the apply for whichever
     * attempt is currently pending (per the metadata store's started/transferred/applied
     * timestamps) -- i.e. that specific apply is not going to complete on its own; only a fresh
     * snapshot sync will make progress again. Lock-free (a volatile read), matching
     * getBaseSnapshotTimestamp()/isProcessingSnapshotSync(): the control-plane thread must never
     * block on this class's monitor to build a metadata response.
     */
    public boolean isApplyRetriesExhausted() {
        return loggedResumeExhaustedForCurrentAttempt;
    }

    /**
     * The minimum time (ms) this sink wants the source to wait before its next SNAPSHOT_START, once
     * isApplyRetriesExhausted() is true -- see LogReplicationConfig.
     * DEFAULT_CHECKPOINTER_GRACE_PERIOD_MS's Javadoc for why. Configured, not state; safe to read
     * lock-free regardless of isApplyRetriesExhausted()'s value (the source only acts on it when
     * that's also true).
     */
    public long getCheckpointerGracePeriodMs() {
        return checkpointerGracePeriodMs;
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
     * applied in stopOnLeadershipLoss(). Also restricted to applyRetrySequenceActive being false, for
     * the identical reason extended to cover the gaps *between* individual resume attempts, not just
     * while one is actively running -- see that field's Javadoc.
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
     *
     * Package-private (not private) so tests can invoke it directly and synchronously instead of
     * waiting out SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS's real scheduler cadence.
     */
    @VisibleForTesting
    void checkSnapshotSyncLiveness() {
        try {
            if (rxState != RxState.SNAPSHOT_SYNC
                    || snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.TRANSFER_PHASE
                    || isWriteOrApplyInFlight()
                    || applyRetrySequenceActive) {
                selfUnfrozeForCurrentIdlePeriod.set(false);
                return;
            }

            synchronized (this) {
                // Re-verify: the fast, unsynchronized check above could be stale by the time we get
                // here (e.g. a fresh SNAPSHOT_START landed in between). Everything from here down is
                // atomic with processSnapshotStart()'s reset.
                if (rxState != RxState.SNAPSHOT_SYNC
                        || snapshotWriter.getPhase() != StreamsSnapshotWriter.Phase.TRANSFER_PHASE
                        || isWriteOrApplyInFlight()
                        || applyRetrySequenceActive) {
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

        // Setup buffer manager. lastSnapshotSyncId was just set above (isValidSnapshotStart()) to
        // this new attempt's id -- passing it lets the buffer manager reject a stale message
        // straggling in from a prior, already-cancelled attempt instead of matching it purely by
        // numeric sequence number (see SnapshotSinkBufferManager's Javadoc on activeSyncRequestId).
        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                logReplicationMetadataManager.getLastSnapshotTransferredSequenceNumber(), lastSnapshotSyncId, this);

        // Set state in SNAPSHOT_SYNC state.
        rxState = RxState.SNAPSHOT_SYNC;
        lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();
        selfUnfrozeForCurrentIdlePeriod.set(false);
        // Defensive: a fresh SNAPSHOT_START (e.g. a forced sync) landing while ongoingApply happened
        // to be momentarily false mid-retry-sequence supersedes that old sequence entirely -- it's
        // being abandoned via this new attempt rather than resumeSnapshotApply()'s own give-up path.
        // onSnapshotSyncStart() below re-freezes (redundantly but harmlessly) regardless, so clearing
        // this doesn't create a gap.
        applyRetrySequenceActive = false;
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
        // Reaching this method at all means snapshotWriter.startSnapshotSyncApply() returned without
        // throwing -- the retry sequence succeeded, so it's over. Clear this before the metadata
        // write/plugin notification below so a fresh SNAPSHOT_START landing concurrently (e.g. for a
        // topology change) isn't held back by a sequence that has, in fact, already finished.
        applyRetrySequenceActive = false;
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
            // Marks the whole retry sequence (not just this one attempt) as active -- see its
            // Javadoc. Set (or re-affirmed) on every attempt, first or resumed; only cleared on
            // genuine success or resumeSnapshotApply() truly giving up.
            applyRetrySequenceActive = true;
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
     *
     * Package-private (not private) so tests can invoke it directly and synchronously instead of
     * waiting out SNAPSHOT_SYNC_LIVENESS_CHECK_INTERVAL_MS's real scheduler cadence.
     */
    @VisibleForTesting
    void checkForStuckApply() {
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
        // come. Resetting ongoingApply below un-gates receive() so a genuinely fresh SNAPSHOT_START
        // is accepted instead of dropped once this retry sequence is truly abandoned (see
        // resumeSnapshotApply()) -- but this specific failure does NOT itself abandon anything or
        // touch the freeze: see the catch block below for why.
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
            log.error("Snapshot sync apply failed for id={}; will retry via resumeSnapshotApply() " +
                    "(subject to its retry cap) before abandoning this attempt.",
                    entry.getMetadata().getSyncRequestId(), t);

            // Deliberately do NOT call onSnapshotSyncEnd() here anymore (unlike the previous design).
            // A failure here does not mean this attempt is abandoned -- resumeSnapshotApply() will
            // retry it in place, reading the SAME shadow-stream data, up to maxSnapshotApplyResumeRetries
            // times. Unfreezing on every individual failure let a compaction cycle both start and
            // complete in the gap before the next retry, and TrimLog.invokePrefixTrim()'s global,
            // address-based trim (not per-stream) would then discard this attempt's shadow-stream
            // data as collateral -- shadow streams are raw, unregistered streams with no per-stream
            // checkpoint protection of their own (see applyRetrySequenceActive's Javadoc). That
            // converted even a transient, otherwise-recoverable failure into a certain one for every
            // subsequent retry in the sequence. applyRetrySequenceActive (left true here) keeps
            // checkpointing frozen through the whole sequence; the eventual, deliberate unfreeze on
            // true give-up happens in resumeSnapshotApply()'s cap-exhausted branch instead, alongside
            // its checkpointerGracePeriodMs signal to the source -- see that method's Javadoc.
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
     * Test-only seam (public: package-private access isn't enough for IT-level tests, which live
     * in a different package) so tests driving a deterministically-failing apply don't have to wait
     * out the full default retry-then-give-up sequence (up to maxSnapshotApplyResumeRetries attempts
     * at the real exponential backoff schedule) to reach resumeSnapshotApply()'s give-up branch.
     */
    @VisibleForTesting
    public void setMaxSnapshotApplyResumeRetries(int maxSnapshotApplyResumeRetries) {
        this.maxSnapshotApplyResumeRetries = maxSnapshotApplyResumeRetries;
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
     * Also the auto-retry path for an apply that previously failed (see startSnapshotApply()'s
     * catch block): LogReplicationServer.handleMetadataRequest() calls this on every metadata poll
     * (~every 2s) while the metadata store shows a transfer-complete-but-not-applied attempt and no
     * apply is currently ongoing.
     *
     * Synchronized on the same monitor as processSnapshotStart() and startSnapshotApply() for two
     * reasons: (1) snapshotWriter.reset() below must not race a concurrent processSnapshotStart()
     * doing the same on a fresh attempt -- previously this method wasn't synchronized at all, so
     * that race was real, not just theoretical; (2) it lets this method authoritatively re-check
     * "is this still actually pending?" using fresh metadata-store reads, since the caller's own
     * check (in handleMetadataRequest(), on the control-plane thread) can be arbitrarily stale by
     * the time this call actually acquires the lock -- a concurrently-arriving fresh SNAPSHOT_START
     * on the data-plane thread may have already started, or even completed, a newer attempt.
     *
     * Bounded by maxSnapshotApplyResumeRetries: once that many consecutive automatic attempts
     * have been made for the same stuck attempt (identified by startedTimestamp) with none of them
     * reaching completeSnapshotApply(), this stops retrying it, unfreezes checkpointing (this is the
     * ONLY place a failed-attempt sequence unfreezes -- see applyRetrySequenceActive's Javadoc for
     * why individual failed attempts along the way deliberately do not), and escalates to a loud,
     * one-time (per stuck attempt) alert instead of continuing forever at the backoff-capped
     * cadence. It also reports checkpointerGracePeriodMs to the source (via the metadata response --
     * see LogReplicationServer.handleMetadataRequest()), asking it to hold off on the next
     * SNAPSHOT_START for at least that long, so the checkpointer that was just unfrozen gets a real,
     * sized window to start and complete a full cycle -- reclaiming this and any prior abandoned
     * attempt's now genuinely safe-to-discard shadow-stream data -- before checkpointing is paused
     * again. Giving up here does not block a genuinely fresh SNAPSHOT_START for a newer attempt: the
     * only sink-side gate on accepting one is ongoingApply (see receive()), which this method never
     * sets once it has given up.
     */
    public synchronized void resumeSnapshotApply() {
        long startedTimestamp = logReplicationMetadataManager.getLastStartedSnapshotTimestamp();
        long transferredTimestamp = logReplicationMetadataManager.getLastTransferredSnapshotTimestamp();
        long appliedTimestamp = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();

        // Re-verify under the lock: the caller's check is stale by construction (computed before
        // acquiring this monitor), and ongoingApply itself can only be trusted authoritatively once
        // we hold this lock, since startSnapshotApplyAsync()/startSnapshotApply() also require it.
        if (ongoingApply.get() || startedTimestamp != transferredTimestamp || transferredTimestamp <= appliedTimestamp) {
            log.info("Skipping resumeSnapshotApply(): no longer pending (or already ongoing) by the " +
                            "time the lock was acquired. started={}, transferred={}, applied={}, ongoingApply={}",
                    startedTimestamp, transferredTimestamp, appliedTimestamp, ongoingApply.get());
            return;
        }

        // Rate-limit: back off exponentially between resume attempts for the SAME stuck attempt
        // (identified by startedTimestamp), mirroring the source side's INITIAL_RETRY_BACKOFF_MS..
        // MAX_RETRY_BACKOFF_MS policy, rather than retrying a possibly-permanently-doomed apply on
        // every ~2s metadata poll forever. A genuinely new attempt (different startedTimestamp)
        // starts its own fresh backoff instead of inheriting a stuck prior attempt's.
        if (startedTimestamp != resumeBackoffForStartedTimestamp) {
            resumeBackoffForStartedTimestamp = startedTimestamp;
            resumeBackoffMs = 0;
            nextResumeAttemptTimeMs = 0;
            resumeAttemptCount = 0;
            loggedResumeExhaustedForCurrentAttempt = false;
        }

        if (resumeAttemptCount >= maxSnapshotApplyResumeRetries) {
            if (!loggedResumeExhaustedForCurrentAttempt) {
                loggedResumeExhaustedForCurrentAttempt = true;
                log.error("Snapshot sync apply for started={} has failed to complete after {} consecutive " +
                                "automatic resume attempts; giving up on auto-retry for this attempt. This " +
                                "attempt's transferred data will sit unapplied until either manual " +
                                "intervention or a fresh snapshot sync (a new SNAPSHOT_START) supersedes it. " +
                                "Unfreezing checkpointing now that the sequence is truly abandoned, and " +
                                "requesting the source hold off on the next attempt for at least {} ms so " +
                                "the checkpointer has a real window to run.",
                        startedTimestamp, resumeAttemptCount, checkpointerGracePeriodMs);
                // The sequence is genuinely over -- this is the one deliberate unfreeze point for a
                // failed-attempt sequence; see applyRetrySequenceActive's Javadoc for why individual
                // attempts along the way don't do this themselves.
                applyRetrySequenceActive = false;
                log.info("Enter onSnapshotSyncEnd (apply retries exhausted) :: {}",
                        snapshotSyncPlugin.getClass().getSimpleName());
                snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
                log.info("Exit onSnapshotSyncEnd (apply retries exhausted) :: {}",
                        snapshotSyncPlugin.getClass().getSimpleName());
            }
            return;
        }

        long now = System.currentTimeMillis();
        if (now < nextResumeAttemptTimeMs) {
            log.debug("Deferring resumeSnapshotApply() for started={}; next attempt allowed in {} ms",
                    startedTimestamp, nextResumeAttemptTimeMs - now);
            return;
        }
        resumeBackoffMs = (resumeBackoffMs == 0)
                ? LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS
                : Math.min(resumeBackoffMs * 2, LogReplicationConfig.MAX_RETRY_BACKOFF_MS);
        nextResumeAttemptTimeMs = now + resumeBackoffMs;
        resumeAttemptCount++;

        // Signal start of snapshot sync to the writer, so data can be cleared (on old snapshot syncs)
        snapshotWriter.reset(topologyConfigId, startedTimestamp);
        // Keep baseSnapshotTimestamp in step with the attempt actually being (re)applied -- e.g.
        // after a sink process restart, this in-memory field defaults to Address.NON_ADDRESS - 1
        // (no processSnapshotStart() has run yet in this process' lifetime for this attempt), which
        // would otherwise make getBaseSnapshotTimestamp() report the wrong attempt identity to a
        // source polling isSinkStillProcessing() for the very attempt this resume is servicing.
        baseSnapshotTimestamp = startedTimestamp;
        UUID snapshotSyncId = new UUID(logReplicationMetadataManager.getCurrentSnapshotSyncCycleId(), Long.MAX_VALUE);
        log.info("Resume Snapshot Sync Apply, snapshot_transfer_ts={}, id={}, backoff={}ms, attempt={}/{}",
                transferredTimestamp, snapshotSyncId, resumeBackoffMs, resumeAttemptCount, maxSnapshotApplyResumeRetries);
        // Construct Log Replication Entry message used to complete the Snapshot Sync with info in the metadata manager
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
                .setTopologyConfigID(logReplicationMetadataManager.getTopologyConfigId())
                .setTimestamp(-1L)
                .setSnapshotTimestamp(transferredTimestamp)
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
            // Also refuses to unfreeze while applyRetrySequenceActive, for the identical reason this
            // already refuses to during APPLY_PHASE -- see that field's Javadoc. Without this, losing
            // leadership in the gap between two resume attempts (phase can read TRANSFER_PHASE there,
            // momentarily, courtesy of resumeSnapshotApply()'s snapshotWriter.reset()) would unfreeze
            // out from under a retry sequence that a metadata poll on this node just isn't going to
            // resume again (handleMetadataRequest() itself is gated on leadership).
            if (snapshotWriter.getPhase() == StreamsSnapshotWriter.Phase.TRANSFER_PHASE && !applyRetrySequenceActive) {
                log.warn("Leadership lost while in TRANSFER phase. Trigger " +
                    "snapshot sync plugin end, to avoid effects of" +
                    "delayed restarts of snapshot sync.");
                log.info("Run onSnapshotSyncEnd :: {}",
                    snapshotSyncPlugin.getClass().getSimpleName());
                snapshotSyncPlugin.onSnapshotSyncEnd(runtime);
                log.info("Completed onSnapshotSyncEnd :: {}",
                    snapshotSyncPlugin.getClass().getSimpleName());
            } else {
                log.warn("Leadership lost while in APPLY phase (or an apply retry sequence is still active). " +
                    "Note that snapshot sync end plugin might not have been run.");
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
