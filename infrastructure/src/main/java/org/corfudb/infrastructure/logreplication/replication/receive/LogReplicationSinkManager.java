package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.Reason;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;

/**
 * This class represents the Log Replication Manager at the destination.
 * It is the entry point for log replication at the receiver.
 *
 * <p>Snapshot sync on the sink is governed by the snapshot lease (see {@link SnapshotLeaseCoordinator}).
 * The sink decides whether a snapshot may start, how long it may keep the checkpointer frozen and
 * what must happen before the next one is admitted. This class owns the writers and buffers; the
 * coordinator owns their lifetime and drives preparation, apply and cleanup on its own thread.
 */
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

    // The buffers are replaced by the coordinator's worker thread (on preparation and on
    // completion) and used by the data-plane thread.
    private volatile LogEntrySinkBufferManager logEntrySinkBufferManager;
    private volatile SnapshotSinkBufferManager snapshotSinkBufferManager;

    private StreamsSnapshotWriter snapshotWriter;
    private LogEntryWriter logEntryWriter;

    @Getter
    private LogReplicationMetadataManager logReplicationMetadataManager;

    // Written by the coordinator's worker thread, read by the data-plane thread.
    private volatile RxState rxState;

    private LogReplicationConfig config;

    // Current topologyConfigId, used to reject out of date messages.
    private volatile long topologyConfigId = 0;

    @VisibleForTesting
    private int rxMessageCounter = 0;

    // Count number of received messages, used for testing purposes
    @VisibleForTesting
    @Getter
    private final ObservableValue<Integer> rxMessageCount = new ObservableValue<>(rxMessageCounter);

    private ISnapshotSyncPlugin snapshotSyncPlugin;

    private volatile SnapshotLeaseCoordinator lifecycle;
    private volatile boolean leadershipGranted;
    private volatile boolean sinkRole = true;
    private SnapshotLeaseCoordinator.Timing leaseTiming = new SnapshotLeaseCoordinator.Timing(
            LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_DURATION_MS,
            LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_MIN_RECOVERY_MS,
            LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_ALARM_MS,
            LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_TRANSFER_IDLE_MS,
            LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_MAX_APPLY_RETRIES);

    // The attempt whose writer is installed. Written by the worker thread (preparation), read by
    // the data-plane thread when the transfer ends.
    private volatile SnapshotSyncLeaseRecord writerAttempt;

    private final String pluginConfigFilePath;

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
     * Test-only constructor that skips the external I/O the two constructors above perform: the
     * runtime is supplied, and so is the plugin, instead of being loaded reflectively from a jar.
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

        // Until the lease reports a completed snapshot and the incremental writer has been installed
        // for it, nothing is admitted. The source queries the receiver's status to decide what type
        // of replication to start with.
        this.rxState = RxState.LOG_ENTRY_SYNC;

        initWriterAndBufferMgr();
        lifecycle = newCoordinator(leaseTiming, SnapshotLeaseCoordinator.RECONCILE_PERIOD_MS);
    }

    private SnapshotLeaseCoordinator newCoordinator(SnapshotLeaseCoordinator.Timing timing, long reconcilePeriodMs) {
        return new SnapshotLeaseCoordinator(runtime, logReplicationMetadataManager, snapshotSyncPlugin,
                new SnapshotLeaseCoordinator.Worker() {
                    public void prepare(SnapshotSyncLeaseRecord attempt) { prepareOwnedSnapshot(attempt); }
                    public void apply(SnapshotSyncLeaseRecord attempt) { applyOwnedSnapshot(attempt); }
                    public void completed(SnapshotSyncLeaseRecord attempt) { installCompletedSnapshot(attempt); }
                }, timing, new SnapshotLeaseCoordinator.Environment(System::currentTimeMillis, System::nanoTime,
                        null, null, null, true, reconcilePeriodMs));
    }

    /**
     * Test-only seam: replaces the lease driver with one using the given timing policy, so tests do
     * not have to wait out production-sized budgets. Only allowed before leadership is granted; a
     * live driver is never swapped out.
     */
    @VisibleForTesting
    public synchronized void configureSnapshotLifecycle(SnapshotLeaseCoordinator.Timing timing) {
        configureSnapshotLifecycle(timing, SnapshotLeaseCoordinator.RECONCILE_PERIOD_MS);
    }

    /**
     * Test-only seam, as above, that also sets the period of the lease driver's reconciliation. With
     * a period far longer than a test, whatever the test sees happen was driven by events alone.
     */
    @VisibleForTesting
    public synchronized void configureSnapshotLifecycle(SnapshotLeaseCoordinator.Timing timing, long reconcilePeriodMs) {
        if (leadershipGranted) {
            throw new IllegalStateException("The snapshot lease driver is already running");
        }
        lifecycle.close();
        leaseTiming = timing;
        lifecycle = newCoordinator(timing, reconcilePeriodMs);
        lifecycle.sinkRole(sinkRole);
    }

    /**
     * Whoever holds the log replication lock of this cluster drives the snapshot lease, whatever the
     * cluster's role. On a sink that is everything a snapshot sync needs. On a cluster that is not a
     * sink (any more) it only settles what a sink left in the record when the role changed: an
     * attempt in flight is abandoned and its protection released, instead of being left to the
     * checkpointer's own backstop an hour or two later.
     */
    public synchronized void setLeadership(boolean leader) {
        leadershipGranted |= leader;
        lifecycle.leadership(leader);
    }

    /** Whether this cluster currently is a sink (standby). See {@link #setLeadership}. */
    public synchronized void setSinkRole(boolean sink) {
        sinkRole = sink;
        lifecycle.sinkRole(sink);
    }

    public SnapshotSyncLeaseRecord getSnapshotLease() {
        return lifecycle.status();
    }

    /** Whether incremental (log entry) traffic would be admitted right now. */
    @VisibleForTesting
    public boolean isIncrementalSyncAdmitted() {
        SnapshotSyncLeaseRecord state = lifecycle.status();
        return state.getOutcome() == Outcome.COMPLETED && state.getPhase() != Phase.NOT_READY
                && lifecycle.incrementalInstalled(state.getGeneration());
    }

    private void prepareOwnedSnapshot(SnapshotSyncLeaseRecord attempt) {
        snapshotWriter.reset(attempt.getTopologyConfigId(), attempt.getSourceSnapshot());
        snapshotWriter.setLeaseContext(attempt);
        writerAttempt = attempt;
        UUID attemptId = getUUID(attempt.getAttemptId());
        snapshotSinkBufferManager = new SnapshotSinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                Address.NON_ADDRESS, attemptId, this);
        rxState = RxState.SNAPSHOT_SYNC;
    }

    private void applyOwnedSnapshot(SnapshotSyncLeaseRecord attempt) {
        ongoingApply.set(true);
        try {
            snapshotWriter.setLeaseContext(attempt);
            if (waitMsBeforeSnapshotApply > 0) {
                log.info("Waiting for {} ms before starting Snapshot Apply", waitMsBeforeSnapshotApply);
                try {
                    TimeUnit.MILLISECONDS.sleep(waitMsBeforeSnapshotApply);
                } catch (InterruptedException e) {
                    // The attempt was abandoned or leadership was lost while waiting: stop before
                    // mutating anything. The coordinator settles the record.
                    throw new SnapshotSyncLease.LeaseRejectedException("Snapshot worker cancelled before apply");
                }
            }
            // Sync with registry after transfer phase to capture local updates, as transfer phase could
            // take a relatively long time.
            config.syncWithRegistry();
            snapshotWriter.clearLocalStreams();
            snapshotWriter.startSnapshotSyncApply();
            LogReplication.LogReplicationEntryMsg end = getLrEntryAckMsg(LogReplicationEntryMetadataMsg.newBuilder()
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_END).setSyncRequestId(attempt.getAttemptId())
                    .setTopologyConfigID(attempt.getTopologyConfigId()).setSnapshotTimestamp(attempt.getSourceSnapshot())
                    .setAttemptGeneration(attempt.getGeneration()).build());
            // Completion, the applied marker and the data-consistent flag commit together, fenced.
            logReplicationMetadataManager.setSnapshotAppliedComplete(end, attempt);
        } finally {
            ongoingApply.set(false);
        }
    }

    /**
     * Installs the incremental writer for the state the lease reports as completed. Runs after a
     * snapshot completes, and again every time this node (re)acquires the lease, because the
     * in-memory positions are stale if another node led in between.
     */
    private void installCompletedSnapshot(SnapshotSyncLeaseRecord attempt) {
        long lastApplied = logReplicationMetadataManager.getLastAppliedSnapshotTimestamp();
        long lastProcessed = logReplicationMetadataManager.getLastProcessedLogEntryBatchTimestamp();
        logEntrySinkBufferManager = new LogEntrySinkBufferManager(ackCycleTime, ackCycleCnt, bufferSize,
                lastProcessed, this);
        logEntryWriter.reset(lastApplied, lastProcessed);
        logEntryWriter.setLeaseContext(attempt);
        rxState = RxState.LOG_ENTRY_SYNC;
        log.info("Incremental writer installed for lease generation {}, snapshot={}, lastProcessed={}",
                attempt.getGeneration(), lastApplied, lastProcessed);
    }

    /**
     * Init the writers, Buffer Manager and Snapshot Plugin.
     */
    private void initWriterAndBufferMgr() {
        // Read config first before init other components.
        readConfig();

        // Instantiate Snapshot Sync Plugin, an external service which is notified when a snapshot
        // attempt acquires and releases its protection. Skipped if a test constructor already
        // supplied one: the real plugin is loaded via reflection from a jar path.
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
        try (FileReader reader = new FileReader(configFile)) {
            Properties props = new Properties();
            props.load(reader);
            bufferSize = Integer.parseInt(props.getProperty("log_reader_max_retry", Integer.toString(bufferSize)));
            ackCycleCnt = Integer.parseInt(props.getProperty("log_writer_ack_cycle_count", Integer.toString(ackCycleCnt)));
            ackCycleTime = Integer.parseInt(props.getProperty("log_writer_ack_cycle_time", Integer.toString(ackCycleTime)));
            leaseTiming = readLeaseTiming(props, leaseTiming);
        } catch (FileNotFoundException e) {
            log.warn("Config file {} does not exist.  Using default configs", CONFIG_FILE);
        } catch (IOException | NumberFormatException e) {
            log.error("Could not read config file {}; using defaults for what was not read", CONFIG_FILE, e);
        }
        log.info("Sink Manager Buffer config queue size {} ackCycleCnt {} ackCycleTime {} snapshotLease {}",
                bufferSize, ackCycleCnt, ackCycleTime, leaseTiming);
    }

    /** A value that would make the budget unbounded or meaningless falls back to its default. */
    @VisibleForTesting
    static SnapshotLeaseCoordinator.Timing readLeaseTiming(Properties props, SnapshotLeaseCoordinator.Timing defaults) {
        long duration = positive(props, "snapshot_lifecycle_max_duration_ms", defaults.getDurationMs());
        long alarm = positive(props, "snapshot_lifecycle_recovery_alarm_ms", defaults.getAlarmMs());
        long recovery = Long.parseLong(props.getProperty("snapshot_lifecycle_min_recovery_ms",
                Long.toString(defaults.getRecoveryMs())));
        long idle = Long.parseLong(props.getProperty("snapshot_lifecycle_transfer_idle_ms",
                Long.toString(defaults.getIdleMs())));
        int retries = Integer.parseInt(props.getProperty("snapshot_lifecycle_max_apply_retries",
                Integer.toString(defaults.getMaxApplyRetries())));
        return new SnapshotLeaseCoordinator.Timing(duration, recovery < 0 ? defaults.getRecoveryMs() : recovery,
                alarm, idle, retries < 0 ? defaults.getMaxApplyRetries() : retries);
    }

    private static long positive(Properties props, String key, long fallback) {
        long value = Long.parseLong(props.getProperty(key, Long.toString(fallback)));
        if (value <= 0) {
            log.error("{}={} is not a finite positive duration; using {}", key, value, fallback);
            return fallback;
        }
        return value;
    }

    /**
     * Receive a message from the sender.
     *
     * <p>Anything that cannot be processed right now is answered with a typed BUSY reply (thrown as
     * {@link org.corfudb.runtime.exceptions.LogReplicationBusyException}) carrying the lease, never
     * by silently dropping the message: the source neither counts it as acknowledged nor as lost.
     *
     * @param message received message
     * @return the acknowledgement, or null when none is due yet
     */
    @Override
    public LogReplication.LogReplicationEntryMsg receive(LogReplication.LogReplicationEntryMsg message) {
        rxMessageCounter++;
        rxMessageCount.setValue(rxMessageCounter);

        LogReplicationEntryMetadataMsg entry = message.getMetadata();
        log.debug("Sink manager received {} while in {}", entry.getEntryType(), rxState);

        SnapshotSyncLeaseRecord state = lifecycle.status();
        // It could be caused by an out-of-date sender or the local node hasn't done the site discovery yet.
        if (entry.getTopologyConfigID() != topologyConfigId) {
            log.warn("Reject message {}. Topology config id mismatch, local={}, msg={}", entry.getEntryType(),
                    topologyConfigId, entry.getTopologyConfigID());
            throw lifecycle.rejected(Reason.STALE_ATTEMPT);
        }
        if (entry.getEntryType() == LogReplicationEntryType.LOG_ENTRY_MESSAGE) {
            // Incremental traffic is only meaningful on top of a fully applied snapshot whose
            // writer this node has installed. The write is bracketed, so that the writer is never
            // replaced, nor the record re-acquired, underneath it.
            if (state.getOutcome() != Outcome.COMPLETED || state.getPhase() == Phase.NOT_READY
                    || !lifecycle.enterIncremental(state.getGeneration())) {
                throw lifecycle.rejected(Reason.ADMISSION_CLOSED);
            }
            try {
                return logEntrySinkBufferManager.processMsgAndBuffer(message);
            } finally {
                lifecycle.exitIncremental();
            }
        }
        if (entry.getSnapshotLifecycleVersion() != SnapshotSyncLease.VERSION) {
            throw lifecycle.rejected(Reason.UNSUPPORTED_PROTOCOL);
        }
        if (entry.getEntryType() == LogReplicationEntryType.SNAPSHOT_START) {
            state = lifecycle.start(entry);
            if (state.getPhase() != Phase.TRANSFERRING && state.getPhase() != Phase.APPLYING) {
                // Reserved, but preparation has not finished. The source retries the same proposal,
                // and soon: preparation has already been started and only takes a moment.
                throw lifecycle.rejected(Reason.ADMISSION_CLOSED, SnapshotLeaseCoordinator.PREPARING_RETRY_AFTER_MS);
            }
            return getLrEntryAckMsg(entry.toBuilder().setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                    .setAttemptGeneration(state.getGeneration()).build());
        }
        // A source that gives up before it has seen its START accepted does not know the generation
        // the sink reserved for it. Its attempt id, which only that source has, identifies the attempt.
        boolean cancelOfUnseenAdmission = entry.getEntryType() == LogReplicationEntryType.SNAPSHOT_CANCEL
                && entry.getAttemptGeneration() == 0;
        if (!SnapshotSyncLease.matches(state, entry)
                || (state.getGeneration() != entry.getAttemptGeneration() && !cancelOfUnseenAdmission)) {
            throw lifecycle.rejected(Reason.STALE_ATTEMPT);
        }
        if (entry.getEntryType() == LogReplicationEntryType.SNAPSHOT_CANCEL) {
            abandonQuietly("Source cancelled its snapshot cut");
            throw lifecycle.rejected(Reason.STALE_ATTEMPT);
        }
        if (entry.getEntryType() == LogReplicationEntryType.SNAPSHOT_END
                && (state.getPhase() == Phase.APPLYING || state.getOutcome() == Outcome.COMPLETED)) {
            // The END reply was lost: the transfer is already durable, so acknowledge it again.
            return getLrEntryAckMsg(entry.toBuilder().setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE)
                    .setSnapshotSyncSeqNum(state.getEndSequence()).build());
        }
        lifecycle.enterTransfer(entry);
        try {
            if (entry.getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE
                    && entry.getEntryType() != LogReplicationEntryType.SNAPSHOT_END) {
                throw lifecycle.rejected(Reason.STALE_ATTEMPT);
            }
            return snapshotSinkBufferManager.processMsgAndBuffer(message);
        } catch (org.corfudb.runtime.exceptions.LogReplicationBusyException e) {
            throw e;
        } catch (StreamsSnapshotWriter.RetryableWriteException e) {
            // Certainly not written (the sequencer refused it, typically because it failed over),
            // and neither the writer nor the buffer has moved on: the source sends it again. Not a
            // reason to throw away a transfer that may have been running for an hour.
            log.warn("Snapshot message {} of generation {} was not written and will be resent: {}",
                    entry.getSnapshotSyncSeqNum(), state.getGeneration(), e.getCause().toString());
            throw lifecycle.rejected(Reason.OVERLOADED);
        } catch (RuntimeException e) {
            // A response failure can leave data committed but its first marker uncertain.
            // Abandon instead of writing that batch a second time into the same shadow range.
            log.warn("Snapshot transfer failed for generation {}", state.getGeneration(), e);
            abandonQuietly("Snapshot transfer failed: " + e.getClass().getSimpleName());
            throw lifecycle.rejected(Reason.STALE_ATTEMPT);
        } finally {
            lifecycle.exitTransfer();
        }
    }

    /**
     * Abandons the attempt on the data path. If the record cannot be updated (for example another
     * node took it over), the source still gets a typed reply rather than a timeout, and the
     * coordinator's next reconciliation settles the record.
     */
    private void abandonQuietly(String reason) {
        try {
            lifecycle.abandon(reason);
        } catch (RuntimeException e) {
            log.warn("Could not abandon the snapshot attempt ({}); reconciliation will settle it", reason, e);
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
                // Moves the lease to APPLYING; the coordinator runs the apply on its own thread.
                lifecycle.transferComplete(writerAttempt, entry.getMetadata().getSnapshotSyncSeqNum());
                break;
            default:
                log.warn("Message type {} should not be applied during snapshot sync.", entry.getMetadata().getEntryType());
                break;
        }
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
     * Test-only seam to inject a substitute snapshot writer (e.g. one that throws on demand), so
     * failure paths that are otherwise only reachable via an actual concurrent trim can be exercised
     * deterministically.
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
        if (this.topologyConfigId != topologyConfigId) {
            abandonQuietly("Topology changed");
        }
        this.topologyConfigId = topologyConfigId;
    }

    /**
     * Invoked on a cluster role or topology change. The lease coordinator owns the lifetime of the
     * writers and buffers: it abandons unfinished work when the topology changes, and reinstalls the
     * incremental writer from the persisted positions whenever this node (re)acquires the lease.
     * Resetting them here as well could corrupt an apply that is still running.
     */
    public void reset() {
        log.debug("Reset Sink Manager: writer state is reinstalled by the snapshot lease coordinator");
    }

    public void shutdown() {
        lifecycle.close();
        this.runtime.shutdown();
    }

    /**
     * Stop any functions on Sink Manager when leadership is lost. The lease stays as it is: the next
     * leader abandons unfinished work when it takes the record over, and if no leader ever returns
     * the checkpointer ignores the protection once it is past its deadline plus the grace.
     */
    public void stopOnLeadershipLoss() {
        setLeadership(false);
    }

    enum RxState {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC
    }
}
