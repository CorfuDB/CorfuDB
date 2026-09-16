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

    public static SnapshotSyncLeaseRecord read(TxnContext txn) {
        var entry = txn.getRecord(TABLE_NAME, DOMAIN);
        SnapshotSyncLeaseRecord state = entry == null || entry.getPayload() == null ? SnapshotSyncLeaseRecord.getDefaultInstance()
                : (SnapshotSyncLeaseRecord) entry.getPayload();
        if (state.getSchemaVersion() > 1) {
            throw new SnapshotSyncLease.LeaseRejectedException("Unsupported snapshot lease schema");
        }
        return state;
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

    /** A cutoff captured before reservation can finish even while a new snapshot is protected. */
    public static boolean permitsTrim(SnapshotSyncLeaseRecord state, long cutoff) {
        return !state.getProtectionHeld() || cutoff <= state.getProtectedAfter();
    }
}
