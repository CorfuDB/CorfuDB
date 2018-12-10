package org.corfudb.runtime.object.transactions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
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
        //TODO(Maithem) move timestamp validation here
        TransactionalContext.newContext(type.get.apply(this));
    }

    public boolean isLoggingEnabled() {
        return runtime.getObjectsView().isTransactionLogging();
    }
}
