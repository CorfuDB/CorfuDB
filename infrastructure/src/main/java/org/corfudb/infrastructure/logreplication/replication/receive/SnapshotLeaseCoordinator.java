package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Sink-local driver of the snapshot lease. The sink, not the source, decides whether a snapshot may
 * start, how long it may keep the checkpointer frozen, when that protection is released and what
 * must happen before the next snapshot is admitted.
 *
 * <p>Polls only read the published view. Storage transitions, worker effects and the nonblocking
 * health watchdog use separate execution paths. A cancelled worker is never replaced until its
 * runnable has actually returned; Future.cancel/isDone cannot prove that it stopped. If it never
 * returns, the checkpointer still ignores the protection once it is past its deadline plus the
 * grace (see {@link SnapshotSyncLeaseStore#protectionActive}).
 */
@Slf4j
public final class SnapshotLeaseCoordinator implements AutoCloseable {

    /** Attempts abandoned in a row after which the sink reports that snapshot sync keeps failing. */
    static final int FAILING_ATTEMPTS_ALARM = 3;

    static final String LEGACY_SUPERSEDED = "Pre-lease snapshot superseded at takeover";

    /** A record found under another owner is not taken back before this long, see {@link #tick()}. */
    static final long REACQUIRE_BACKOFF_MS = 5000;

    /**
     * Pacing of the checkpoint requests of the recovery gate. The first request goes out at once;
     * each further one waits twice as long as the previous, so that a cycle that keeps failing is not
     * run back to back, up to about the cadence at which the compactor runs cycles on its own.
     */
    static final long CHECKPOINT_REQUEST_BACKOFF_MS = TimeUnit.MINUTES.toMillis(1);
    static final long CHECKPOINT_REQUEST_MAX_BACKOFF_MS = TimeUnit.MINUTES.toMillis(15);

    /** How long the trim that follows a satisfying cycle may take before another cycle is requested. */
    static final long TRIM_WAIT_MS = TimeUnit.MINUTES.toMillis(2);

    private static final int MAX_TABLES_IN_DETAIL = 10;
    private static final long RETRY_AFTER_MS = 2000;

    /** Timing policy of the lease. A non-positive {@code idleMs} disables inactivity abandonment. */
    @Value
    public static class Timing {
        /** Total budget of one attempt: preparation, transfer, apply and apply retries. */
        long durationMs;
        /** Minimum time protection stays released before admission may reopen. */
        long recoveryMs;
        /** How long cleanup or recovery may take before it is reported. */
        long alarmMs;
        /** How long an admitted attempt may go without accepted snapshot traffic. */
        long idleMs;
        /** Retries of a transiently failing apply, all inside the same budget. */
        int maxApplyRetries;
    }

    @lombok.AllArgsConstructor
    static class Environment {
        LongSupplier clock;
        LongSupplier ticker;
        SnapshotSyncLeaseStore store;
        DistributedCheckpointerHelper checkpointer;
        ExecutorService executor;
        boolean scheduled;
    }

    public interface Worker {
        void prepare(SnapshotSyncLeaseRecord attempt);
        void apply(SnapshotSyncLeaseRecord attempt);
        void completed(SnapshotSyncLeaseRecord attempt);
    }

    private final CorfuRuntime runtime;
    private final LogReplicationMetadataManager metadata;
    private final SnapshotSyncLeaseStore store;
    private final ISnapshotSyncPlugin plugin;
    private final Worker worker;
    private final DistributedCheckpointerHelper checkpointer;
    private final Timing timing;
    private final String owner = UUID.randomUUID().toString();
    private final AtomicReference<SnapshotSyncLeaseRecord> view = new AtomicReference<>(
            SnapshotSyncLeaseRecord.getDefaultInstance());
    private final AtomicBoolean busy = new AtomicBoolean();
    private final AtomicBoolean receiving = new AtomicBoolean();
    // An incremental (log entry) write is in flight on the data plane. It does not hold back the
    // reconciliation as a whole, which must go on releasing protection under steady traffic; it only
    // keeps the record from being re-acquired, and the incremental writer from being replaced,
    // underneath that write.
    private final AtomicBoolean incremental = new AtomicBoolean();
    private final AtomicLong leadershipEpoch = new AtomicLong();
    private final LongSupplier clock;
    private final LongSupplier ticker;
    private final boolean scheduled;

    private volatile boolean leader;
    private volatile boolean initialized;
    private volatile Thread workerThread;
    private volatile long lastSuccessfulTickMs;
    private volatile long deadlineNanos = Long.MAX_VALUE;
    private volatile long timedGeneration = -1;
    private volatile long activityGeneration = -1;
    private volatile long lastActivityNanos;
    private volatile String recoveryDetail = "";
    private volatile long installedGeneration = -1;
    // Whether this cluster is a sink. The lock holder of a cluster that is not (any more) still
    // settles what its sink left in the record, but it neither admits nor asks for checkpoints.
    private volatile boolean sinkRole = true;
    private volatile long reacquireNotBeforeNanos;
    private volatile boolean reacquireDelayed;
    private long describedFailedCycle = -1;
    private boolean bypassLogged;

    // State of the recovery gate, only touched by the transitions thread.
    private long gateGeneration = -1;
    private long gateRecoveryCut = -1;
    private long satisfiedCut = -1;
    private long satisfiedSinceNanos;
    private long checkpointRequestBackoffMs;
    private long nextCheckpointRequestNanos;
    private boolean checkpointRequested;
    private volatile String reportedBlockedDescription;

    // Observability. Gauges are backed by these holders and refreshed on every reconciliation.
    private final AtomicInteger recoveryBlocked = new AtomicInteger();
    private final AtomicInteger snapshotFailing = new AtomicInteger();
    private final AtomicInteger consecutiveAborts = new AtomicInteger();
    private final AtomicLong budgetRemainingMs = new AtomicLong();
    private final List<Meter> meters = new ArrayList<>();
    private Phase observedPhase = Phase.NOT_READY;
    private long observedPhaseSinceNanos;

    private final ExecutorService effects;
    private final ScheduledExecutorService transitions = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-transitions-%d").build());
    private final ScheduledExecutorService health = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-health-%d").build());

    public SnapshotLeaseCoordinator(CorfuRuntime runtime, LogReplicationMetadataManager metadata,
                                    ISnapshotSyncPlugin plugin, Worker worker, Timing timing) {
        this(runtime, metadata, plugin, worker, timing,
                new Environment(System::currentTimeMillis, System::nanoTime, null, null, null, true));
    }

    SnapshotLeaseCoordinator(CorfuRuntime runtime, LogReplicationMetadataManager metadata,
                             ISnapshotSyncPlugin plugin, Worker worker, Timing timing, Environment environment) {
        if (timing.getDurationMs() <= 0 || timing.getRecoveryMs() < 0 || timing.getAlarmMs() <= 0
                || timing.getMaxApplyRetries() < 0) {
            throw new IllegalArgumentException("The snapshot lease requires a finite budget and alarm interval");
        }
        clock = environment.clock;
        ticker = environment.ticker;
        scheduled = environment.scheduled;
        lastSuccessfulTickMs = clock.getAsLong();
        lastActivityNanos = ticker.getAsLong();
        observedPhaseSinceNanos = ticker.getAsLong();
        effects = environment.executor != null ? environment.executor : Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-effects-%d").build());
        this.runtime = runtime;
        this.metadata = metadata;
        this.store = environment.store != null ? environment.store : new SnapshotSyncLeaseStore(metadata.getCorfuStore());
        this.plugin = plugin;
        this.worker = worker;
        this.timing = timing;
        registerGauge("logreplication.snapshot.recovery.blocked", recoveryBlocked);
        registerGauge("logreplication.snapshot.sync.failing", snapshotFailing);
        registerGauge("logreplication.snapshot.lease.consecutive.aborts", consecutiveAborts);
        registerGauge("logreplication.snapshot.lease.budget.remaining.ms", budgetRemainingMs);
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.LOG_REPLICATION));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.LOG_REPLICATION));
        try {
            checkpointer = environment.checkpointer != null ? environment.checkpointer
                    : new DistributedCheckpointerHelper(metadata.getCorfuStore());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot initialize snapshot recovery", e);
        }
        if (scheduled) {
            transitions.scheduleWithFixedDelay(this::tick, 0, 1, TimeUnit.SECONDS);
            health.scheduleWithFixedDelay(this::checkHealth, 1, 1, TimeUnit.SECONDS);
        }
    }

    private void registerGauge(String name, Number holder) {
        MeterRegistryProvider.getInstance().ifPresent(registry -> meters.add(
                Gauge.builder(name, holder, Number::doubleValue).strongReference(true).register(registry)));
    }

    public SnapshotSyncLeaseRecord status() {
        return leader && initialized ? view.get() : SnapshotSyncLeaseRecord.getDefaultInstance();
    }

    /** Whether the incremental writer is installed for the completed snapshot of {@code generation}. */
    public boolean incrementalInstalled(long generation) {
        return leader && initialized && installedGeneration == generation;
    }

    /**
     * Brackets an incremental write on the data plane. The flag is raised before the installation
     * is checked, and the transitions thread invalidates the installation before it looks at the
     * flag, so at least one of the two always sees the other.
     */
    public boolean enterIncremental(long generation) {
        incremental.set(true);
        if (incrementalInstalled(generation)) {
            return true;
        }
        incremental.set(false);
        return false;
    }

    public void exitIncremental() {
        incremental.set(false);
    }

    public void sinkRole(boolean sink) {
        sinkRole = sink;
    }

    public void leadership(boolean value) {
        leadershipEpoch.incrementAndGet();
        leader = value;
        if (!value) {
            initialized = false;
            installedGeneration = -1;
            interruptWorker();
            clearObservations();
        } else if (scheduled) {
            // Take over without waiting for the next periodic reconciliation.
            try {
                transitions.execute(this::tick);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                log.debug("Snapshot lease driver is closed; ignoring leadership", e);
            }
        }
    }

    private void publish(SnapshotSyncLeaseRecord state) {
        view.updateAndGet(old -> {
            if (state.getRevision() < old.getRevision()) { return old; }
            if (SnapshotSyncLease.active(state)) {
                long candidate = ticker.getAsLong() + TimeUnit.MILLISECONDS.toNanos(
                        Math.max(0, state.getDeadlineMs() - clock.getAsLong()));
                deadlineNanos = timedGeneration == state.getGeneration() ? Math.min(deadlineNanos, candidate) : candidate;
                timedGeneration = state.getGeneration();
            }
            return state;
        });
    }

    /** Traffic of the admitted attempt was accepted: the source is alive. */
    private void touchActivity(long generation) {
        // The time first: whoever reads the new generation must never pair it with an older time.
        lastActivityNanos = ticker.getAsLong();
        activityGeneration = generation;
    }

    public SnapshotSyncLeaseRecord start(LogReplicationEntryMetadataMsg entry) {
        if (!leader || !initialized) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED);
        }
        SnapshotSyncLeaseRecord current = view.get();
        if (SnapshotSyncLease.matches(current, entry)) {
            if (SnapshotSyncLease.active(current)) {
                touchActivity(current.getGeneration());
                return current; // Duplicate START never reinitializes or extends the attempt.
            }
            throw rejected(LogReplicationBusyResponseMsg.Reason.STALE_ATTEMPT);
        }
        if (busy.get() || receiving.get()) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED);
        }
        try {
            SnapshotSyncLeaseRecord reserved = store.updateOwned(owner, (txn, state) -> {
                SnapshotSyncLeaseRecord next = SnapshotSyncLease.reserve(state, entry,
                        clock.getAsLong(), timing.getDurationMs(), txn.getTxnSequence(), UUID.randomUUID().toString());
                metadata.initializeSnapshot(txn, entry);
                return next;
            });
            touchActivity(reserved.getGeneration());
            publish(reserved);
            return reserved;
        } catch (SnapshotSyncLease.LeaseRejectedException e) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED);
        } catch (TransactionAbortedException e) {
            // Lost a commit race, typically against incremental entries that were still in flight
            // when the source switched to a snapshot. The source retries the same proposal.
            log.debug("Snapshot reservation lost a commit race; the source will retry", e);
            throw rejected(LogReplicationBusyResponseMsg.Reason.OVERLOADED);
        }
    }

    public SnapshotSyncLeaseRecord enterTransfer(LogReplicationEntryMetadataMsg entry) {
        if (!receiving.compareAndSet(false, true)) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.OVERLOADED);
        }
        SnapshotSyncLeaseRecord state = status();
        if (state.getPhase() != Phase.TRANSFERRING || !SnapshotSyncLease.matches(state, entry)
                || state.getGeneration() != entry.getAttemptGeneration()) {
            receiving.set(false);
            throw rejected(LogReplicationBusyResponseMsg.Reason.STALE_ATTEMPT);
        }
        touchActivity(state.getGeneration());
        return state;
    }

    public void exitTransfer() {
        lastActivityNanos = ticker.getAsLong();
        receiving.set(false);
    }

    public void transferComplete(SnapshotSyncLeaseRecord captured, long endSequence) {
        publish(store.updateOwned(owner, (txn, state) -> {
            SnapshotSyncLease.checkWriter(state, captured, clock.getAsLong(), Phase.TRANSFERRING);
            // Marks the sink's data inconsistent in the same transaction: apply comes next.
            metadata.transferSnapshot(txn, state.getSourceSnapshot());
            return SnapshotSyncLease.transferred(state).toBuilder().setEndSequence(endSequence).build();
        }));
    }

    public void abandon(String reason) {
        if (initialized) {
            abandonOwned(reason);
        }
    }

    private void abandonOwned(String reason) {
        publish(store.updateOwned(owner, (txn, state) -> {
            SnapshotSyncLeaseRecord abandoned = SnapshotSyncLease.abandon(state, clock.getAsLong(), reason);
            if (abandoned != state) {
                log.warn("Abandoning snapshot attempt generation={} in phase {}: {}",
                        state.getGeneration(), state.getPhase(), reason);
                metadata.abandonSnapshot(txn);
            }
            return abandoned;
        }));
        interruptWorker();
    }

    public LogReplicationBusyException rejected(LogReplicationBusyResponseMsg.Reason reason) {
        return new LogReplicationBusyException(LogReplicationBusyResponseMsg.newBuilder()
                .setReason(reason).setSnapshotLease(status()).setRetryAfterMs(RETRY_AFTER_MS).build());
    }

    void tick() {
        long epoch = leadershipEpoch.get();
        if (!leader) {
            return;
        }
        try {
            if (!initialized) {
                // Another node may have led in between and moved the replicated state on, so the
                // incremental writer is reinstalled from the persisted positions on every takeover.
                // Invalidated before the data plane is looked at, see enterIncremental().
                installedGeneration = -1;
                if (busy.get() || receiving.get() || incremental.get()) {
                    // A writer of this JVM outlived a leadership loss. It is never reset underneath
                    // itself, but its budget still applies while it drains.
                    expireWhileDraining();
                    return;
                }
                if (reacquireDelayed && ticker.getAsLong() - reacquireNotBeforeNanos < 0) {
                    return;
                }
                reacquireDelayed = false;
                SnapshotSyncLeaseRecord acquired = store.update(this::acquire);
                // What this node cached while it last led must not be served as current.
                metadata.refreshSnapshotStatus();
                if (epoch != leadershipEpoch.get()) {
                    // Leadership changed while the record was being acquired. This pass must not
                    // leave the node initialized: the next leadership would then skip acquire(),
                    // and with it the fencing and the checks a takeover exists for.
                    return;
                }
                publish(acquired);
                initialized = true;
            }
            SnapshotSyncLeaseRecord current = store.read();
            if (!current.getOwnerId().equals(owner)) {
                // Another node took the record over. Writers of this node are already fenced by
                // the owner check. Take it back if this node still leads, instead of going silent
                // until leadership happens to be toggled again, but not at once: while a lock changes
                // hands two nodes can believe they lead for a while, and taking the record back and
                // forth every second would abandon every attempt the legitimate leader admits.
                log.error("Snapshot lease is owned by {} while this node ({}) leads log replication; re-acquiring in {} ms",
                        current.getOwnerId(), owner, REACQUIRE_BACKOFF_MS);
                initialized = false;
                installedGeneration = -1;
                reacquireNotBeforeNanos = ticker.getAsLong() + TimeUnit.MILLISECONDS.toNanos(REACQUIRE_BACKOFF_MS);
                reacquireDelayed = true;
                interruptWorker();
                return;
            }
            publish(current);
            long now = clock.getAsLong();
            if (SnapshotSyncLease.active(current)) {
                String expiry = expiryReason(current, now);
                if (expiry != null) {
                    abandonOwned(expiry);
                    current = view.get();
                }
            }
            if (cleanupOverdue(current, now)) {
                publish(store.updateOwned(owner, (txn, state) -> cleanupOverdue(state, now)
                        ? SnapshotSyncLease.faulted(state) : state));
                current = view.get();
            }
            if (!busy.get() && !receiving.get()) {
                reconcile(current, now);
            }
            metadata.refreshSnapshotStatus();
            observe(view.get(), now);
            lastSuccessfulTickMs = now;
        } catch (Throwable e) {
            // Anything that escapes a periodic task cancels it for good, and with it the only thing
            // that releases protection on this side.
            log.warn("Snapshot lifecycle reconciliation will retry", e);
        }
    }

    private void reconcile(SnapshotSyncLeaseRecord current, long now) {
        if (current.getOutcome() == Outcome.COMPLETED && installedGeneration != current.getGeneration()) {
            if (incremental.get()) {
                return; // Never replace the incremental writer underneath a write that is in flight.
            }
            long epoch = leadershipEpoch.get();
            execute(() -> {
                worker.completed(current);
                if (epoch == leadershipEpoch.get()) {
                    installedGeneration = current.getGeneration();
                }
            });
            return;
        }
        switch (current.getPhase()) {
            case PREPARING:
                execute(() -> prepare(view.get()));
                break;
            case APPLYING:
                if (now >= current.getRetryAtMs()) {
                    execute(() -> apply(view.get()));
                }
                break;
            case ABORTING:
            case RELEASING:
            case FAULTED:
                execute(this::release);
                break;
            case RECOVERING:
                recover(now);
                break;
            default:
                break;
        }
    }

    /**
     * First reconciliation after this node became the log replication leader of the sink.
     *
     * <p>Without a record this is the first activation on this sink, reached through a rolling
     * upgrade. A snapshot the pre-lease protocol left unfinished is superseded rather than waited
     * for: nothing in this version can finish it, so waiting would close replication forever. Its
     * leftover freeze token is deleted, because the hook that would have deleted it is never called
     * again, and admission reopens only after one checkpoint and trim have reclaimed what it left.
     *
     * <p>With a record, recovery conservatively abandons unfinished work. It never grants more
     * time, assumes a first-shadow marker exists, or reuses a previous owner's writer. During a
     * rolling upgrade leadership can bounce back to a node that still runs the pre-lease protocol,
     * which ignores the record. A snapshot such a node left unfinished is recognized by its source
     * timestamp, which is not the one of the record's own last attempt, and is superseded as well.
     *
     * <p>Either way the previous leader's incremental writer is fenced in this transaction.
     */
    private SnapshotSyncLeaseRecord acquire(TxnContext txn, SnapshotSyncLeaseRecord state) {
        long now = clock.getAsLong();
        metadata.fenceIncrementalWriters(txn);
        if (state.getSchemaVersion() == 0) {
            if (metadata.legacySnapshotPending(txn)) {
                deleteLegacyFreezeToken(txn);
                metadata.abandonSnapshot(txn);
                log.warn("Superseding the snapshot the pre-lease protocol left unfinished; "
                        + "admission reopens after one checkpoint and trim");
                return SnapshotSyncLease.seedRecovering(owner, now, txn.getTxnSequence(),
                        metadata.lastStartedSnapshot(txn), LEGACY_SUPERSEDED);
            }
            return SnapshotSyncLease.seedIdle(owner, metadata.lastAppliedSnapshot(txn),
                    metadata.persistedTopologyConfigId(txn));
        }
        long lastStarted = metadata.lastStartedSnapshot(txn);
        boolean foreign = metadata.legacySnapshotPending(txn) && lastStarted != state.getSourceSnapshot();
        SnapshotSyncLeaseRecord acquired = SnapshotSyncLease.next(state).setOwnerId(owner).build();
        if (SnapshotSyncLease.active(acquired)) {
            acquired = SnapshotSyncLease.abandon(acquired, now, "Sink ownership changed");
            metadata.abandonSnapshot(txn);
        }
        if (foreign) {
            deleteLegacyFreezeToken(txn);
            metadata.abandonSnapshot(txn);
            log.warn("Superseding a snapshot that a pre-lease node left unfinished while it led this sink "
                    + "(lease generation={}, phase={})", acquired.getGeneration(), acquired.getPhase());
            acquired = SnapshotSyncLease.supersededByLegacy(acquired, now, txn.getTxnSequence(), lastStarted,
                    LEGACY_SUPERSEDED);
        }
        return acquired;
    }

    /**
     * Only in the unfinished pre-lease case is the token known to be log replication's. Otherwise
     * it may be an operator freeze, which is left to the compactor's own patience.
     */
    private void deleteLegacyFreezeToken(TxnContext txn) {
        RpcCommon.TokenMsg token = (RpcCommon.TokenMsg) txn.getRecord(
                CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN).getPayload();
        if (token != null) {
            txn.delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
            log.warn("Deleted the checkpointer freeze token the pre-lease snapshot sync left behind (frozen at {})",
                    new Date(token.getSequence()));
        }
    }

    private void expireWhileDraining() {
        SnapshotSyncLeaseRecord stored = store.read();
        long now = clock.getAsLong();
        if (stored.getOwnerId().equals(owner) && SnapshotSyncLease.active(stored)
                && (now >= stored.getDeadlineMs() || now < stored.getAdmittedAtMs())) {
            abandonOwned("Snapshot resource deadline expired");
        }
    }

    private String expiryReason(SnapshotSyncLeaseRecord current, long now) {
        if (now >= current.getDeadlineMs() || now < current.getAdmittedAtMs() || ticker.getAsLong() >= deadlineNanos) {
            return "Snapshot resource deadline expired";
        }
        boolean awaitingSource = current.getPhase() == Phase.PREPARING || current.getPhase() == Phase.TRANSFERRING;
        if (timing.getIdleMs() <= 0 || !awaitingSource || receiving.get()) {
            return null;
        }
        if (activityGeneration != current.getGeneration()) {
            touchActivity(current.getGeneration());
            return null;
        }
        long idleNanos = ticker.getAsLong() - lastActivityNanos;
        return idleNanos >= TimeUnit.MILLISECONDS.toNanos(timing.getIdleMs())
                ? "No snapshot traffic accepted for " + TimeUnit.NANOSECONDS.toMillis(idleNanos) + " ms" : null;
    }

    private void execute(Runnable action) {
        if (!busy.compareAndSet(false, true)) {
            return;
        }
        try {
            effects.execute(() -> {
                workerThread = Thread.currentThread();
                try {
                    action.run();
                } catch (Throwable e) {
                    log.warn("Snapshot effect did not complete; reconciliation remains pending", e);
                } finally {
                    workerThread = null;
                    Thread.interrupted();
                    busy.set(false);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            busy.set(false); // Closed: nothing will run, and nothing must look busy forever.
            log.debug("Snapshot lease driver is closed; effect not run", e);
        }
    }

    private void prepare(SnapshotSyncLeaseRecord captured) {
        if (captured.getPhase() != Phase.PREPARING) {
            // Dispatched on a record read before the previous effect committed: already prepared.
            // Running it again would reset the writers underneath a transfer that has started.
            return;
        }
        try {
            plugin.acquireSnapshot(runtime, captured);
            worker.prepare(captured);
            publish(store.updateOwned(owner, (txn, state) -> {
                SnapshotSyncLease.checkWriter(state, captured, clock.getAsLong(), Phase.PREPARING);
                return SnapshotSyncLease.prepared(state);
            }));
            touchActivity(captured.getGeneration());
        } catch (Throwable e) {
            // Errors included (a plugin may fail an assertion): an attempt that cannot be prepared
            // must be abandoned, not retried every second until its deadline.
            log.warn("Snapshot preparation failed for generation {}", captured.getGeneration(), e);
            abandon("Snapshot preparation failed: " + e.getClass().getSimpleName());
        }
    }

    private void apply(SnapshotSyncLeaseRecord captured) {
        if (captured.getPhase() != Phase.APPLYING) {
            return;
        }
        try {
            if (captured.getTransferredSequence() >= 0 && (captured.getFirstShadowAddress() < 0
                    || runtime.getAddressSpaceView().getTrimMark().getSequence() > captured.getFirstShadowAddress())) {
                abandon("Snapshot shadow data is no longer retained");
                return;
            }
            worker.apply(captured);
            publish(store.read()); // Worker commits completion and consistency in the same fenced transaction.
        } catch (TrimmedException | SnapshotSyncLease.LeaseRejectedException e) {
            abandon("Snapshot apply abandoned: " + e.getClass().getSimpleName());
        } catch (Throwable e) {
            // Errors included: they count as a failed try and back off like any other, instead of
            // being dispatched again every second.
            log.warn("Snapshot apply failed for generation {} (retries so far: {})", captured.getGeneration(),
                    captured.getApplyRetries(), e);
            if (captured.getApplyRetries() >= timing.getMaxApplyRetries()) {
                abandon("Snapshot apply abandoned: " + e.getClass().getSimpleName());
            } else {
                publish(store.updateOwned(owner, (txn, state) -> {
                    SnapshotSyncLease.checkWriter(state, captured, clock.getAsLong(), Phase.APPLYING);
                    long delay = Math.min(60000L, 2000L << Math.min(5, state.getApplyRetries()));
                    return SnapshotSyncLease.next(state).setApplyRetries(state.getApplyRetries() + 1)
                            .setRetryAtMs(clock.getAsLong() + delay)
                            .setFailure(e.getClass().getSimpleName()).build();
                }));
            }
        }
    }

    private void release() {
        Phase phase = store.read().getPhase();
        if (phase != Phase.ABORTING && phase != Phase.RELEASING && phase != Phase.FAULTED) {
            return; // Dispatched on a record read before the previous release committed.
        }
        SnapshotSyncLeaseRecord drained = store.updateOwned(owner, (txn, state) ->
                SnapshotSyncLease.drained(state, txn.getTxnSequence()).toBuilder().setCleanupStartedAtMs(
                        state.getCleanupStartedAtMs() == 0 ? clock.getAsLong() : state.getCleanupStartedAtMs()).build());
        publish(drained);
        plugin.releaseSnapshot(runtime, drained);
        publish(store.updateOwned(owner, (txn, state) -> {
            if (!state.getProtectionId().equals(drained.getProtectionId())) {
                throw new SnapshotSyncLease.LeaseRejectedException("Protection owner changed");
            }
            return SnapshotSyncLease.released(state, clock.getAsLong());
        }));
    }

    /**
     * Admission reopens once a checkpoint cycle that started after the recovery cut has completed
     * and the log has been trimmed past that cut. The sink asks for that cycle itself instead of
     * waiting for the compactor's own cadence, see {@link #requestCheckpoint}.
     */
    private void recover(long now) {
        CheckpointingStatus cycle;
        RpcCommon.TokenMsg cut;
        try (TxnContext txn = metadata.getTxnContext()) {
            cycle = (CheckpointingStatus) txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            cut = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.MIN_CHECKPOINT).getPayload();
            txn.commit();
        }
        String bypass = !sinkRole ? "this cluster is not a sink"
                : cycle == null ? "compaction never ran on this cluster"
                : checkpointer.isCompactionDisabled() ? "compaction is disabled" : null;
        if (bypass != null) {
            // No checkpointer can run: there is nothing to starve and no trim will ever pass the
            // cut, so waiting would only keep replication closed forever. A missing manager record
            // is a reliable sign of that: the compaction service creates it (as IDLE) on its first
            // pass, seconds after the server starts and even while checkpointing is frozen, so a
            // configured compactor can never be mistaken for an absent one and starved this way.
            // On a cluster that is not a sink no snapshot can be admitted in the first place, and a
            // forced checkpoint with an immediate trim is not something to impose on a source.
            if (!bypassLogged) {
                log.warn("Reopening snapshot admission without a checkpoint: {}", bypass);
                bypassLogged = true;
            }
            publish(store.updateOwned(owner, (txn, state) -> state.getPhase() == Phase.RECOVERING
                    ? SnapshotSyncLease.recoveredWithoutCheckpoint(state) : state));
            recoveryDetail = "";
            return;
        }
        bypassLogged = false;
        SnapshotSyncLeaseRecord current = view.get();
        if (gateGeneration != current.getGeneration() || gateRecoveryCut != current.getRecoveryCut()) {
            // Another recovery than the one the state below was kept for.
            gateGeneration = current.getGeneration();
            gateRecoveryCut = current.getRecoveryCut();
            satisfiedCut = -1;
            checkpointRequested = false;
            checkpointRequestBackoffMs = 0;
        }
        if (cycle.getStatus() == StatusType.COMPLETED && cut != null && cut.getSequence() >= current.getRecoveryCut()
                && satisfiedCut < cut.getSequence()) {
            // Remembered: a later cycle that fails does not take back what this one reclaimed.
            if (satisfiedCut < 0) {
                satisfiedSinceNanos = ticker.getAsLong();
            }
            satisfiedCut = cut.getSequence();
        }
        boolean trimmed = false;
        if (satisfiedCut >= 0) {
            // Only now is the trim mark worth a round trip to the log units.
            long completedCut = satisfiedCut;
            long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();
            trimmed = trimMark > current.getRecoveryCut();
            publish(store.updateOwned(owner, (txn, state) -> SnapshotSyncLease.recovered(state, now,
                    timing.getRecoveryMs(), completedCut, trimMark)));
        }
        if (view.get().getPhase() != Phase.RECOVERING) {
            recoveryDetail = "";
            return;
        }
        recoveryDetail = describeRecoveryBlock(cycle, cut, current);
        boolean trimPending = satisfiedCut >= 0 && !trimmed
                && ticker.getAsLong() - satisfiedSinceNanos < TimeUnit.MILLISECONDS.toNanos(TRIM_WAIT_MS);
        if (trimmed || trimPending) {
            // A satisfying cycle has completed. What is missing is its trim, which the compactor
            // runs right after a requested cycle, or only the minimum interval. Another cycle would
            // be a full checkpoint for nothing.
            return;
        }
        requestCheckpoint(now);
    }

    /**
     * Asks the compactor for a cycle with trim through the durable trigger it already honors. The
     * compactor deletes that trigger whenever a cycle ends, whatever its outcome, so a request that
     * was simply repeated as soon as the trigger is gone would run cycles back to back for as long as
     * they fail: every one of them writes checkpoint data, and none of them trims.
     */
    private void requestCheckpoint(long now) {
        long nowNanos = ticker.getAsLong();
        if (checkpointRequested && nowNanos - nextCheckpointRequestNanos < 0) {
            return;
        }
        boolean requested = false;
        try (TxnContext txn = metadata.getTxnContext()) {
            if (txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM).getPayload() == null) {
                txn.putRecord(checkpointer.getCompactorMetadataTables().getCompactionControlsTable(),
                        CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM,
                        RpcCommon.TokenMsg.newBuilder().setSequence(now).build(), null);
                requested = true;
            }
            txn.commit();
        }
        if (requested) {
            checkpointRequestBackoffMs = checkpointRequested
                    ? Math.min(CHECKPOINT_REQUEST_MAX_BACKOFF_MS, Math.max(CHECKPOINT_REQUEST_BACKOFF_MS, checkpointRequestBackoffMs * 2))
                    : CHECKPOINT_REQUEST_BACKOFF_MS;
            checkpointRequested = true;
            nextCheckpointRequestNanos = nowNanos + TimeUnit.MILLISECONDS.toNanos(checkpointRequestBackoffMs);
            log.info("Requested a checkpoint cycle with trim past {}; the next request, if this one does not "
                    + "reopen snapshot admission, is not before {} ms", view.get().getRecoveryCut(), checkpointRequestBackoffMs);
        }
    }

    private String describeRecoveryBlock(CheckpointingStatus cycle, RpcCommon.TokenMsg cut,
                                         SnapshotSyncLeaseRecord current) {
        if (cycle.getStatus() != StatusType.FAILED) {
            return "waiting for a checkpoint past " + current.getRecoveryCut() + " (last cycle " + cycle.getStatus()
                    + ", cut " + (cut == null ? -1 : cut.getSequence()) + ")";
        }
        if (cycle.getCycleCount() == describedFailedCycle && !recoveryDetail.isEmpty()) {
            return recoveryDetail;
        }
        describedFailedCycle = cycle.getCycleCount();
        List<String> failing = new ArrayList<>();
        try (TxnContext txn = metadata.getTxnContext()) {
            for (TableName table : txn.keySet(checkpointer.getCompactorMetadataTables().getCheckpointingStatusTable())) {
                CheckpointingStatus status = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                if (status != null && status.getStatus() != StatusType.COMPLETED) {
                    failing.add(table.getNamespace() + "$" + table.getTableName() + "=" + status.getStatus());
                }
            }
            txn.commit();
        } catch (Exception e) {
            log.debug("Unable to list the tables of the failed checkpoint cycle", e);
        }
        return "last checkpoint cycle FAILED" + (failing.isEmpty() ? "" : ": " + failing.stream()
                .limit(MAX_TABLES_IN_DETAIL).collect(Collectors.joining(", "))
                + (failing.size() > MAX_TABLES_IN_DETAIL ? ", ..." : ""));
    }

    private void interruptWorker() {
        Thread thread = workerThread;
        if (thread != null && thread.threadId() != Thread.currentThread().threadId()) {
            thread.interrupt();
        }
    }

    private boolean cleanupOverdue(SnapshotSyncLeaseRecord state, long now) {
        return (state.getPhase() == Phase.ABORTING && now - state.getAbortedAtMs() >= timing.getAlarmMs())
                || (state.getPhase() == Phase.RELEASING && state.getCleanupStartedAtMs() > 0
                    && now - state.getCleanupStartedAtMs() >= timing.getAlarmMs());
    }

    /** Refreshes the gauges and records how long the previous phase lasted. */
    private void observe(SnapshotSyncLeaseRecord state, long now) {
        consecutiveAborts.set(state.getConsecutiveAborts());
        budgetRemainingMs.set(SnapshotSyncLease.active(state) ? Math.max(0, state.getDeadlineMs() - now) : 0);
        if (state.getPhase() != observedPhase) {
            long nowNanos = ticker.getAsLong();
            long elapsed = Math.max(0, nowNanos - observedPhaseSinceNanos);
            Phase finished = observedPhase;
            MeterRegistryProvider.getInstance().ifPresent(registry -> recordPhase(registry, finished, elapsed));
            observedPhase = state.getPhase();
            observedPhaseSinceNanos = nowNanos;
        }
    }

    private static void recordPhase(MeterRegistry registry, Phase phase, long elapsedNanos) {
        registry.timer("logreplication.snapshot.lease.phase.duration", "phase", phase.name())
                .record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Logs and gauges are what this process can offer: the health monitor only exists in the Corfu
     * server process, so the reports below have no effect unless one is initialized here. The
     * compactor leader, which runs there and reads the same durable record on every pass, raises the
     * corresponding health issues (see CompactorLeaderServices#checkForProlongedFreeze).
     */
    void checkHealth() {
        try {
            evaluateHealth();
        } catch (Throwable e) {
            // Anything that escapes a periodic task cancels it for good.
            log.warn("Snapshot lease health evaluation failed; it is retried", e);
        }
    }

    private void evaluateHealth() {
        if (!leader) {
            return;
        }
        long now = clock.getAsLong();
        SnapshotSyncLeaseRecord state = view.get();
        boolean cleanupBlocked = cleanupOverdue(state, now) || state.getPhase() == Phase.FAULTED;
        boolean recoveryOverdue = sinkRole && state.getPhase() == Phase.RECOVERING
                && now - state.getReleasedAtMs() >= timing.getAlarmMs();
        boolean driverBlocked = now - lastSuccessfulTickMs >= timing.getAlarmMs();
        if (cleanupBlocked || recoveryOverdue || driverBlocked) {
            String description = "Snapshot admission blocked: phase=" + state.getPhase() + ", generation="
                    + state.getGeneration() + ", recoveryCut=" + state.getRecoveryCut() + ", failure=" + state.getFailure()
                    + (driverBlocked ? ", the lease driver has not completed a pass for "
                            + (now - lastSuccessfulTickMs) + " ms" : "")
                    + (recoveryDetail.isEmpty() ? "" : ", " + recoveryDetail);
            if (recoveryBlocked.getAndSet(1) == 0) {
                log.error("SNAPSHOT_RECOVERY_BLOCKED: {}", description);
            }
            if (!description.equals(reportedBlockedDescription)) {
                // The health status keeps the description an issue was first reported with.
                if (reportedBlockedDescription != null) {
                    HealthMonitor.resolveIssue(blockedIssue(reportedBlockedDescription));
                }
                reportedBlockedDescription = description;
            }
            HealthMonitor.reportIssue(blockedIssue(description));
        } else if (recoveryBlocked.getAndSet(0) != 0) {
            log.info("SNAPSHOT_RECOVERY_BLOCKED resolved: phase={}", state.getPhase());
            HealthMonitor.resolveIssue(blockedIssue(reportedBlockedDescription));
            reportedBlockedDescription = null;
        }

        Issue failing = Issue.createIssue(Component.LOG_REPLICATION, Issue.IssueId.SNAPSHOT_SYNC_FAILING,
                state.getConsecutiveAborts() + " snapshot attempts were abandoned in a row, last failure="
                        + state.getFailure());
        if (state.getConsecutiveAborts() >= FAILING_ATTEMPTS_ALARM) {
            if (snapshotFailing.getAndSet(1) == 0) {
                log.error("SNAPSHOT_SYNC_FAILING: {}", failing);
            }
            HealthMonitor.reportIssue(failing);
        } else if (snapshotFailing.getAndSet(0) != 0) {
            log.info("SNAPSHOT_SYNC_FAILING resolved by a completed snapshot");
            HealthMonitor.resolveIssue(failing);
        }
    }

    private static Issue blockedIssue(String description) {
        return Issue.createIssue(Component.LOG_REPLICATION, Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED,
                description == null ? "" : description);
    }

    /** A node that does not lead knows nothing current: it must not keep reporting what it last saw. */
    private void clearObservations() {
        if (recoveryBlocked.getAndSet(0) != 0) {
            HealthMonitor.resolveIssue(blockedIssue(reportedBlockedDescription));
        }
        reportedBlockedDescription = null;
        if (snapshotFailing.getAndSet(0) != 0) {
            HealthMonitor.resolveIssue(Issue.createIssue(Component.LOG_REPLICATION,
                    Issue.IssueId.SNAPSHOT_SYNC_FAILING, ""));
        }
        consecutiveAborts.set(0);
        budgetRemainingMs.set(0);
        recoveryDetail = "";
    }

    @Override
    public void close() {
        leadership(false);
        transitions.shutdownNow();
        health.shutdownNow();
        effects.shutdownNow();
        MeterRegistryProvider.getInstance().ifPresent(registry -> meters.forEach(registry::remove));
    }
}
