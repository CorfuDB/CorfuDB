package org.corfudb.runtime.object.transactions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;


/**
 * Represents a transaction.
 *
 * <p>Created by Maithem on 12/5/18.
 */

@Getter
@AllArgsConstructor
@Builder
public class Transaction {

    /**
     * The runtime for the context.
     */
    @NonNull
    final CorfuRuntime runtime;

    /**
     * The type of context to build.
     */
    @Default
    final TransactionType type = TransactionType.OPTIMISTIC;;

    /**
     * For snapshot transactions, the address the
     * snapshot will start at.
     */
    @Default
    final Token snapshot = Token.UNINITIALIZED;;

    /**
     * Start the transaction with the parameters given
     * to the builder.
     */
    public void begin() {
        verify();
        TransactionalContext.newContext(type.get.apply(this));
    }

    public boolean isLoggingEnabled() {
        return runtime.getObjectsView().isTransactionLogging();
    }

    /**
     * Verifies that this transaction  has a valid snapshot and context.
     */
    private void verify() {
        final AbstractTransactionalContext parentCtx = TransactionalContext.getCurrentContext();
        if (parentCtx != null) {
            if (parentCtx.getTransaction().getRuntime() != this.runtime) {
                throw new IllegalStateException("Attempting to nest transactions from different clients!");
            }
            // If we're in a nested transaction, the first read timestamp
            // needs to come from the root.
            Token parentTimestamp = parentCtx.getSnapshotTimestamp();
            if (!this.snapshot.equals(Token.UNINITIALIZED)
                    && !this.snapshot.equals(parentTimestamp)) {
                String msg = String.format("Attempting to nest transactions with" +
                                " different timestamps, parent ts=%s, user defined ts=%s", parentCtx.getSnapshotTimestamp(),
                        this.snapshot);
                throw new IllegalArgumentException(msg);
            }
        }
        // User-Defined Snapshot Verification
        //        else if (!this.snapshot.equals(Token.UNINITIALIZED)) {
        //            // Since this is a user-defined snapshot we must make sure that
        //            // the corresponding log position is valid (i.e. the slot has been written)
        //            ILogData pos = getRuntime().getAddressSpaceView().peek(this.snapshot.getSequence());
        //            if (pos == null || !pos.getToken().equals(this.snapshot)) {
        //                String msg = String.format("User Defined snapshot(ts=%s) is invalid", this.snapshot);
        //                throw new IllegalArgumentException(msg);
        //            }
        //        }
    }
}
