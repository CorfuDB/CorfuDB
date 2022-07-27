package org.corfudb.runtime.object;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.WriteSetSMRStream;
import org.corfudb.runtime.view.Address;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * An implementation of ISnapshotProxy specialized for the VersionLockedObject.
 * @param <T> The type of the underlying SMR object.
 */
@Slf4j
@NotThreadSafe
public class VersionLockedSnapshotProxy<T extends ICorfuSMR<T>> implements ISnapshotProxy<T> {

    /**
     * The VersionLockedObject that maintains the underlying object corresponding
     * to this snapshot proxy.
     */
    private final VersionLockedObject<T> versionLockedObject;

    /**
     * The global log position that the transaction uses to serve all accesses.
     */
    private final Token snapshotTimestamp;

    /**
     * The transactional context of the transaction that requested this snapshot proxy.
     */
    private final AbstractTransactionalContext txnContext;

    /**
     * The last known stream position accessed during this transaction.
     */
    @Getter
    private Long lastKnownStreamPosition;

    public VersionLockedSnapshotProxy(@Nonnull VersionLockedObject<T> versionLockedObject,
                                      @Nonnull Token snapshotTimestamp,
                                      @Nonnull AbstractTransactionalContext txnContext) {
        this.versionLockedObject = versionLockedObject;
        this.snapshotTimestamp = snapshotTimestamp;
        this.txnContext = txnContext;

        this.lastKnownStreamPosition = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(@Nonnull ICorfuSMRAccess<R, T> accessFunction) {
        // If the transaction is read-only, it will not have optimistic updates.
        Runnable optimisticStreamSetter = null;
        if (!txnContext.readOnly()) {
            optimisticStreamSetter = versionLockedObject::setUncommittedChanges;
        }

        final Runnable finalOptimisticStreamSetter = optimisticStreamSetter;
        return versionLockedObject.access(
                this::directAccessCheckUnsafe,
                vlo -> syncWithRetryUnsafe(vlo, finalOptimisticStreamSetter),
                accessFunction::access,
                this::updateKnownStreamPosition
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getUpcallResult(long timestamp) {
        if (txnContext.readOnly()) {
            throw new UnsupportedOperationException("Can't get upcall during a read-only transaction!");
        }

        // If we have a result, return it.
        SMREntry wrapper = getWriteSetEntryList().get((int) timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()) {
            return wrapper.getUpcallResult();
        }

        return versionLockedObject.update(vlo -> {
            log.trace("Upcall[{}] {} Sync'd", txnContext, timestamp);
            syncWithRetryUnsafe(vlo, vlo::setUncommittedChanges);
            SMREntry wrapper2 = getWriteSetEntryList().get((int) timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()) {
                return wrapper2.getUpcallResult();
            }

            // If we still don't have the upcall, this must be a bug.
            throw new IllegalStateException("Tried to get upcall during a transaction but"
                    + " we don't have it even after an optimistic sync (asked for " + timestamp
                    + " we have 0-" + (getWriteSetEntryList().size() - 1) + ")");
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void logUpdate(@Nonnull SMREntry updateEntry) {
        if (txnContext.readOnly()) {
            throw new UnsupportedOperationException("Can't modify object during a read-only transaction!");
        }
    }

    private void updateKnownStreamPosition(long position) {
        if (lastKnownStreamPosition == null) {
            lastKnownStreamPosition = position;
        }

        Preconditions.checkState(lastKnownStreamPosition == position,
                "inconsistent stream positions %s and %s", lastKnownStreamPosition, position);
    }

    private List<SMREntry> getWriteSetEntryList() {
        return txnContext.getWriteSetInfo().getWriteSet().getSMRUpdates(versionLockedObject.getID());
    }

    private boolean directAccessCheckUnsafe(VersionLockedObject<T> vlo) {
        final WriteSetSMRStream stream = vlo.getOptimisticStreamUnsafe();

        // Obtain the stream position as the transaction context last remembered it.
        final long streamReadPosition = lastKnownStreamPosition == null ?
                snapshotTimestamp.getSequence() : lastKnownStreamPosition;

        final boolean hasExpectedOptimisticStream = stream == null || stream.isStreamCurrentContextThreadCurrentContext();

        final boolean hasExpectedOptimisticUpdates = stream != null && getWriteSetEntryList().size() == stream.pos() + 1
                || getWriteSetEntryList().isEmpty() && vlo.getVersionUnsafe() == streamReadPosition;

        return hasExpectedOptimisticStream && hasExpectedOptimisticUpdates;
    }

    /**
     * Sync the object, which will bring the object to the correct version, reflecting
     * any optimistic updates in the process.
     */
    private void syncWithRetryUnsafe(VersionLockedObject<T> vlo, @Nullable Runnable optimisticStreamSetter) {
        final int trimRetry = txnContext.getTransaction().getRuntime().getParameters().getTrimRetry();

        for (int x = 0; x < trimRetry; x++) {
            try {
                if (optimisticStreamSetter != null) {
                    // Swap ourselves to be the active optimistic stream.
                    // Inside setAsOptimisticStream, if there are
                    // currently optimistic updates on the object, we
                    // roll them back. Then, we set this context as the
                    // object's new optimistic context.
                    optimisticStreamSetter.run();
                }

                // Inside syncObjectUnsafe, depending on the object version,
                // we may need to undo or redo committed changes.
                vlo.syncObjectUnsafe(snapshotTimestamp.getSequence());
                break;
            } catch (TrimmedException te) {
                log.info("syncWithRetryUnsafe: Encountered trimmed address space " +
                                "for snapshot {} of stream {} with pointer={} on attempt {}",
                        snapshotTimestamp.getSequence(), vlo.getID(), vlo.getVersionUnsafe(), x);

                // If a trim is encountered, we must reset the object.
                vlo.resetUnsafe();

                if (!te.isRetriable() || x == trimRetry - 1) {
                    // Abort the transaction.
                    TransactionAbortedException tae = new TransactionAbortedException(
                            new TxResolutionInfo(txnContext.getTransactionID(), snapshotTimestamp),
                            TokenResponse.NO_CONFLICT_KEY,
                            vlo.getID(),
                            Address.NON_ADDRESS,
                            AbortCause.TRIM,
                            te,
                            txnContext
                    );

                    txnContext.abortTransaction(tae);
                    throw tae;
                }
            }
        }
    }
}
