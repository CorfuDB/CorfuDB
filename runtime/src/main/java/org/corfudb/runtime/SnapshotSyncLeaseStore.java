package org.corfudb.runtime;

import com.google.protobuf.Message;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.function.BiFunction;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/** Shared transactional admission and retention record; never reset by LR topology migration. */
public final class SnapshotSyncLeaseStore {
    public static final String TABLE_NAME = "SnapshotSyncLeaseTable";
    public static final StringKey DOMAIN = StringKey.newBuilder().setKey("sink-freeze-domain").build();

    /** System property overriding {@link #DEFAULT_EXPIRY_GRACE_MS} for the checkpointer-side backstop. */
    public static final String EXPIRY_GRACE_PROPERTY = "corfu.snapshot.lease.expiry.grace.ms";

    /**
     * How long past its deadline held protection is still honored by the checkpointer. The sink
     * abandons and releases an attempt at its deadline on its own; this grace only matters when the
     * sink cannot (hung worker, no log replication leader). It must comfortably exceed the clock
     * skew between cluster nodes.
     */
    public static final long DEFAULT_EXPIRY_GRACE_MS = 30L * 60L * 1000L;

    private final CorfuStore store;

    public SnapshotSyncLeaseStore(CorfuStore store) {
        this.store = store;
        open(store);
    }

    public static Table<StringKey, SnapshotSyncLeaseRecord, Message> open(CorfuStore store) {
        try {
            return store.openTable(CORFU_SYSTEM_NAMESPACE, TABLE_NAME, StringKey.class,
                    SnapshotSyncLeaseRecord.class, null, TableOptions.fromProtoSchema(SnapshotSyncLeaseRecord.class));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot open snapshot retention metadata", e);
        }
    }

    /**
     * For whoever drives or follows the lease. A record written by a newer version is refused: its
     * transitions cannot be reasoned about by this one.
     */
    public static SnapshotSyncLeaseRecord read(TxnContext txn) {
        SnapshotSyncLeaseRecord state = readForRetention(txn);
        if (state.getSchemaVersion() > SnapshotSyncLease.VERSION) {
            throw new SnapshotSyncLease.LeaseRejectedException("Unsupported snapshot lease schema");
        }
        return state;
    }

    /**
     * For the checkpointer, which only needs to know what must not be trimmed, and until when:
     * {@code protectionHeld}, {@code protectedAfter} and {@code deadlineMs}, whose meaning every
     * schema version keeps. Refusing a newer record here would stop compaction altogether, silently,
     * for as long as the checkpointer is older than the sink (every rolling upgrade, and for good
     * after a rollback), which is the very failure the lease exists to prevent. Fields this version
     * does not know survive a rewrite of the record.
     */
    public static SnapshotSyncLeaseRecord readForRetention(TxnContext txn) {
        var entry = txn.getRecord(TABLE_NAME, DOMAIN);
        return entry == null || entry.getPayload() == null ? SnapshotSyncLeaseRecord.getDefaultInstance()
                : (SnapshotSyncLeaseRecord) entry.getPayload();
    }

    public static void write(TxnContext txn, SnapshotSyncLeaseRecord state) {
        txn.putRecord(txn.getTable(TABLE_NAME), DOMAIN, state, null);
    }

    /**
     * CorfuStore uses WRITE_AFTER_WRITE transactions. A read alone is NOT a conflict fence.
     * Reappend the observed lease in every protected mutation, so abandonment, ownership changes,
     * and competing admission serialize with the actual data write at the sequencer.
     */
    public static SnapshotSyncLeaseRecord fence(TxnContext txn, SnapshotSyncLeaseRecord captured,
                                                long now, SnapshotSyncLeaseRecord.Phase phase) {
        SnapshotSyncLeaseRecord state = read(txn);
        SnapshotSyncLease.checkWriter(state, captured, now, phase);
        write(txn, state);
        return state;
    }

    public SnapshotSyncLeaseRecord read() {
        try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            SnapshotSyncLeaseRecord state = read(txn);
            txn.commit();
            return state;
        }
    }

    /** The callback must only append transactional effects, never perform external I/O. */
    public SnapshotSyncLeaseRecord update(BiFunction<TxnContext, SnapshotSyncLeaseRecord,
            SnapshotSyncLeaseRecord> transition) {
        for (int retry = 0; ; retry++) {
            try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
                SnapshotSyncLeaseRecord previous = read(txn);
                SnapshotSyncLeaseRecord next = transition.apply(txn, previous);
                if (!next.equals(previous)) {
                    write(txn, next);
                }
                txn.commit();
                return next;
            } catch (TransactionAbortedException e) {
                if (retry >= 2 || Thread.currentThread().isInterrupted()) {
                    throw e;
                }
            }
        }
    }

    public SnapshotSyncLeaseRecord updateOwned(String owner,
            BiFunction<TxnContext, SnapshotSyncLeaseRecord, SnapshotSyncLeaseRecord> transition) {
        return update((txn, state) -> {
            if (!state.getOwnerId().equals(owner)) {
                throw new SnapshotSyncLease.LeaseRejectedException("Snapshot lease owner changed");
            }
            return transition.apply(txn, state);
        });
    }

    public static long expiryGraceMs() {
        long configured = Long.getLong(EXPIRY_GRACE_PROPERTY, DEFAULT_EXPIRY_GRACE_MS);
        return configured < 0 ? DEFAULT_EXPIRY_GRACE_MS : configured;
    }

    /**
     * Whether the checkpointer must yield to this record at wall-clock time {@code now}. Protection
     * that outlived its deadline plus the grace is ignored: see
     * {@link SnapshotSyncLease#protectionExpired}.
     */
    public static boolean protectionActive(SnapshotSyncLeaseRecord state, long now) {
        return state.getProtectionHeld() && !SnapshotSyncLease.protectionExpired(state, now, expiryGraceMs());
    }

    /** A cutoff captured before reservation can finish even while a new snapshot is protected. */
    public static boolean permitsTrim(SnapshotSyncLeaseRecord state, long cutoff, long now) {
        return !protectionActive(state, now) || cutoff <= state.getProtectedAfter();
    }
}
