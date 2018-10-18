package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

/**
 * Helper class to build transactional contexts.
 *
 * <p>Created by mwei on 11/21/16.
 */
@Getter
public class Transaction {

    /**
     * The runtime for the context.
     */
    final CorfuRuntime runtime;

    /**
     * The type of context to build.
     */
    final TransactionType type;

    /**
     * For snapshot transactions, the address the
     * snapshot will start at.
     */
    final long snapshot;

    public Transaction(CorfuRuntime runtime, TransactionType type, long snapshot) {
        this.runtime = runtime;
        this.type = type;
        this.snapshot = snapshot;
    }

    /**
     * Start the transaction with the parameters given
     * to the builder.
     */
    public void begin() {
        TransactionalContext.newContext(type.get.apply(this));
    }

    public static class TransactionBuilder {
        private CorfuRuntime runtime;
        private TransactionType type = TransactionType.OPTIMISTIC;
        private long snapshot = Address.NON_ADDRESS;

        public static TransactionBuilder builder() {
            return new TransactionBuilder();
        }

        TransactionBuilder() {
        }

        public TransactionBuilder runtime(CorfuRuntime runtime) {
            this.runtime = runtime;
            return this;
        }

        public TransactionBuilder type(TransactionType type) {
            this.type = type;
            return this;
        }

        public TransactionBuilder snapshot(long snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Transaction build() {
            return new Transaction(runtime, type, snapshot);
        }
    }
}
