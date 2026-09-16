package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Sink-local driver. Polls only read its published view. Storage transitions, worker effects and
 * the nonblocking health watchdog use separate execution paths. A cancelled worker is never replaced
 * until its runnable has actually returned; Future.cancel/isDone cannot prove that it stopped.
 */
@Slf4j
public final class SnapshotLeaseCoordinator implements AutoCloseable {
    private final CorfuRuntime runtime;
    private final LogReplicationMetadataManager metadata;
    private final SnapshotSyncLeaseStore store;
    private final ISnapshotSyncPlugin plugin;
    private final Worker worker;
    private final DistributedCheckpointerHelper checkpointer;
    private final long durationMs;
    private final long recoveryMs;
    private final long alarmMs;
    private final int maxRetries;
    private final String owner = UUID.randomUUID().toString();
    private final AtomicReference<SnapshotSyncLeaseRecord> view = new AtomicReference<>(
            SnapshotSyncLeaseRecord.getDefaultInstance());
    private final AtomicBoolean busy = new AtomicBoolean();
    private final AtomicBoolean receiving = new AtomicBoolean();
    private final java.util.concurrent.atomic.AtomicInteger recoveryBlocked = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.Optional<io.micrometer.core.instrument.Gauge> recoveryGauge;
    private volatile boolean leader;
    private volatile boolean initialized;
    private volatile Thread workerThread;
    private volatile long lastSuccessfulTickMs;
    private final java.util.function.LongSupplier clock;
    private final java.util.function.LongSupplier ticker;

    private volatile long deadlineNanos = Long.MAX_VALUE;
    private volatile long timedGeneration = -1;
    private long installedGeneration = -1;
    private final ExecutorService effects;
    private final ScheduledExecutorService transitions = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-transitions-%d").build());
    private final ScheduledExecutorService health = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-health-%d").build());

    @lombok.AllArgsConstructor
    static class Environment {
        java.util.function.LongSupplier clock;
        java.util.function.LongSupplier ticker;
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

    public SnapshotLeaseCoordinator(CorfuRuntime runtime, LogReplicationMetadataManager metadata,
                                    ISnapshotSyncPlugin plugin, Worker worker, long durationMs,
                                    long recoveryMs, long alarmMs, int maxRetries) {
        this(runtime, metadata, plugin, worker, durationMs, recoveryMs, alarmMs, maxRetries,
                new Environment(System::currentTimeMillis, System::nanoTime, null, null, null, true));
    }

    SnapshotLeaseCoordinator(CorfuRuntime runtime, LogReplicationMetadataManager metadata,
                             ISnapshotSyncPlugin plugin, Worker worker, long durationMs,
                             long recoveryMs, long alarmMs, int maxRetries, Environment environment) {
        clock = environment.clock;
        ticker = environment.ticker;
        lastSuccessfulTickMs = clock.getAsLong();
        effects = environment.executor != null ? environment.executor : Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-lease-effects-%d").build());
        if (durationMs <= 0 || recoveryMs < 0 || alarmMs <= 0 || maxRetries < 0
                || !plugin.supportsOwnedSnapshotLifecycle()) {
            throw new IllegalArgumentException("Snapshot lifecycle requires finite timing and an owned plugin");
        }
        this.runtime = runtime;
        this.metadata = metadata;
        this.store = environment.store != null ? environment.store : new SnapshotSyncLeaseStore(metadata.getCorfuStore());
        this.plugin = plugin;
        this.worker = worker;
        this.durationMs = durationMs;
        this.recoveryMs = recoveryMs;
        this.alarmMs = alarmMs;
        this.maxRetries = maxRetries;
        recoveryGauge = org.corfudb.common.metrics.micrometer.MeterRegistryProvider.getInstance().map(registry ->
                io.micrometer.core.instrument.Gauge.builder("logreplication.snapshot.recovery.blocked", recoveryBlocked,
                        java.util.concurrent.atomic.AtomicInteger::doubleValue).strongReference(true).register(registry));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.LOG_REPLICATION));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.LOG_REPLICATION));
        try {
            checkpointer = environment.checkpointer != null ? environment.checkpointer : new DistributedCheckpointerHelper(metadata.getCorfuStore());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot initialize snapshot recovery", e);
        }
        if (environment.scheduled) {
            transitions.scheduleWithFixedDelay(this::tick, 0, 1, TimeUnit.SECONDS);
            health.scheduleWithFixedDelay(this::checkHealth, 1, 1, TimeUnit.SECONDS);
        }
    }

    public SnapshotSyncLeaseRecord status() {
        return leader && initialized ? view.get() : SnapshotSyncLeaseRecord.getDefaultInstance();
    }

    public void leadership(boolean value) {
        leader = value;
        if (!value) {
            initialized = false;
            interruptWorker();
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

    public SnapshotSyncLeaseRecord start(LogReplicationEntryMetadataMsg entry) {
        if (!leader || !initialized) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED);
        }
        SnapshotSyncLeaseRecord current = view.get();
        if (SnapshotSyncLease.matches(current, entry)) {
            if (SnapshotSyncLease.active(current)) {
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
                        clock.getAsLong(), durationMs, txn.getTxnSequence(), UUID.randomUUID().toString());
                metadata.initializeSnapshot(txn, entry);
                return next;
            });
            publish(reserved);
            return reserved;
        } catch (SnapshotSyncLease.LeaseRejectedException e) {
            throw rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED);
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
        return state;
    }

    public void exitTransfer() {
        receiving.set(false);
    }

    public void transferComplete(SnapshotSyncLeaseRecord captured, long endSequence) {
        publish(store.updateOwned(owner, (txn, state) -> {
            SnapshotSyncLease.checkWriter(state, captured, clock.getAsLong(), Phase.TRANSFERRING);
            metadata.transferSnapshot(txn, state.getSourceSnapshot());
            return SnapshotSyncLease.transferred(state).toBuilder().setEndSequence(endSequence).build();
        }));
    }

    public void abandon(String reason) {
        if (initialized) {
            publish(store.updateOwned(owner, (txn, state) -> {
                SnapshotSyncLeaseRecord abandoned = SnapshotSyncLease.abandon(state, clock.getAsLong(), reason);
                if (abandoned != state) {
                    metadata.abandonSnapshot(txn);
                }
                return abandoned;
            }));
            interruptWorker();
        }
    }

    public LogReplicationBusyException rejected(LogReplicationBusyResponseMsg.Reason reason) {
        return new LogReplicationBusyException(LogReplicationBusyResponseMsg.newBuilder()
                .setReason(reason).setSnapshotLease(status()).setRetryAfterMs(2000).build());
    }

    void tick() {
        if (!leader) {
            return;
        }
        try {
            if (!initialized) {
                // Do not reset a writer still running in this JVM after loss/reacquisition.
                if (busy.get() || receiving.get()) {
                    return;
                }
                publish(store.update((txn, state) -> {
                    if (state.getSchemaVersion() == 0) {
                        if (metadata.legacySnapshotPending(txn) || checkpointer.isCheckpointFrozen(txn)) {
                            throw new IllegalStateException("Finish legacy snapshot cleanup before enabling lifecycle");
                        }
                        return SnapshotSyncLease.initial(owner);
                    }
                    SnapshotSyncLeaseRecord acquired = SnapshotSyncLease.next(state).setOwnerId(owner).build();
                    // Recovery conservatively abandons unfinished work. It never grants more time,
                    // assumes a first-shadow marker exists, or reuses a previous owner's writer.
                    if (SnapshotSyncLease.active(acquired)) {
                        acquired = SnapshotSyncLease.abandon(acquired, clock.getAsLong(), "Sink ownership changed");
                        metadata.abandonSnapshot(txn);
                    }
                    return acquired;
                }));
                initialized = true;
            }
            SnapshotSyncLeaseRecord current = store.read();
            if (!current.getOwnerId().equals(owner)) {
                leadership(false);
                return;
            }
            publish(current);
            long now = clock.getAsLong();
            if (SnapshotSyncLease.active(current)
                    && (now >= current.getDeadlineMs() || now < current.getAdmittedAtMs() || ticker.getAsLong() >= deadlineNanos)) {
                abandon("Snapshot resource deadline expired");
                current = view.get();
            }
            if (cleanupOverdue(current, now)) {
                publish(store.updateOwned(owner, (txn, state) -> cleanupOverdue(state, now)
                        ? SnapshotSyncLease.faulted(state) : state));
                current = view.get();
            }
            if (!busy.get() && !receiving.get()) {
                if (current.getOutcome() == Outcome.COMPLETED && installedGeneration != current.getGeneration()) {
                    SnapshotSyncLeaseRecord completed = current;
                    execute(() -> {
                        worker.completed(completed);
                        installedGeneration = completed.getGeneration();
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
            metadata.refreshSnapshotStatus();
            lastSuccessfulTickMs = now;
        } catch (Exception e) {
            log.warn("Snapshot lifecycle reconciliation will retry", e);
        }
    }

    private void execute(Runnable action) {
        if (!busy.compareAndSet(false, true)) {
            return;
        }
        effects.execute(() -> {
            workerThread = Thread.currentThread();
            try {
                action.run();
            } catch (Exception e) {
                log.warn("Snapshot effect did not complete; reconciliation remains pending", e);
            } finally {
                workerThread = null;
                Thread.interrupted();
                busy.set(false);
            }
        });
    }

    private void prepare(SnapshotSyncLeaseRecord captured) {
        try {
            plugin.acquireSnapshot(runtime, captured);
            worker.prepare(captured);
            publish(store.updateOwned(owner, (txn, state) -> {
                SnapshotSyncLease.checkWriter(state, captured, clock.getAsLong(), Phase.PREPARING);
                return SnapshotSyncLease.prepared(state);
            }));
        } catch (Exception e) {
            abandon("Snapshot preparation failed: " + e.getClass().getSimpleName());
        }
    }

    private void apply(SnapshotSyncLeaseRecord captured) {
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
        } catch (Exception e) {
            if (captured.getApplyRetries() >= maxRetries) {
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

    private void recover(long now) {
        long safeCut = -1;
        try (TxnContext txn = metadata.getTxnContext()) {
            CheckpointingStatus cycle = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            RpcCommon.TokenMsg cut = (RpcCommon.TokenMsg) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.MIN_CHECKPOINT).getPayload();
            if (cycle != null && cycle.getStatus() == CheckpointingStatus.StatusType.COMPLETED && cut != null) {
                safeCut = cut.getSequence();
            }
            txn.commit();
        }
        long completedCut = safeCut;
        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();
        publish(store.updateOwned(owner, (txn, state) -> SnapshotSyncLease.recovered(state, now,
                recoveryMs, completedCut, trimMark)));
        if (view.get().getPhase() == Phase.RECOVERING) {
            // Coalesce into one durable trigger; don't re-stamp it on every status tick.
            try (TxnContext txn = metadata.getTxnContext()) {
                if (txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                        CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM).getPayload() == null) {
                    txn.putRecord(checkpointer.getCompactorMetadataTables().getCompactionControlsTable(),
                            CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM,
                            RpcCommon.TokenMsg.newBuilder().setSequence(now).build(), null);
                }
                txn.commit();
            }
        }
    }

    private void interruptWorker() {
        Thread thread = workerThread;
        if (thread != null && thread.threadId() != Thread.currentThread().threadId()) {
            thread.interrupt();
        }
    }

    private boolean cleanupOverdue(SnapshotSyncLeaseRecord state, long now) {
        return (state.getPhase() == Phase.ABORTING && now - state.getAbortedAtMs() >= alarmMs)
                || (state.getPhase() == Phase.RELEASING && state.getCleanupStartedAtMs() > 0
                    && now - state.getCleanupStartedAtMs() >= alarmMs);
    }

    void checkHealth() {
        if (!leader) {
            return;
        }
        long now = clock.getAsLong();
        SnapshotSyncLeaseRecord state = view.get();
        boolean cleanupBlocked = cleanupOverdue(state, now) || state.getPhase() == Phase.FAULTED;
        boolean recoveryBlocked = state.getPhase() == Phase.RECOVERING && now - state.getReleasedAtMs() >= alarmMs;
        boolean driverBlocked = now - lastSuccessfulTickMs >= alarmMs;
        Issue issue = Issue.createIssue(Component.LOG_REPLICATION, Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED,
                "Snapshot admission blocked: phase=" + state.getPhase() + ", generation=" + state.getGeneration()
                        + ", recoveryCut=" + state.getRecoveryCut() + ", failure=" + state.getFailure());
        if (cleanupBlocked || recoveryBlocked || driverBlocked) {
            if (this.recoveryBlocked.getAndSet(1) == 0) {
                log.error("SNAPSHOT_RECOVERY_BLOCKED: {}", issue);
            }
            HealthMonitor.reportIssue(issue);
        } else if (state.getPhase() == Phase.READY) {
            if (this.recoveryBlocked.getAndSet(0) != 0) {
                log.info("SNAPSHOT_RECOVERY_BLOCKED resolved after checkpoint and trim recovery");
            }
            HealthMonitor.resolveIssue(issue);
        }
    }

    @Override
    public void close() {
        leadership(false);
        transitions.shutdownNow();
        health.shutdownNow();
        effects.shutdownNow();
        org.corfudb.common.metrics.micrometer.MeterRegistryProvider.getInstance().ifPresent(registry ->
                recoveryGauge.ifPresent(registry::remove));
    }
}
