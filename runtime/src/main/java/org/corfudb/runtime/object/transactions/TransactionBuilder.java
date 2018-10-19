package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

/** Helper class to build transactional contexts.
 *
 * <p>Created by mwei on 11/21/16.
 */
@Accessors(chain = true)
@Setter
@Getter
public class TransactionBuilder {

    /** The runtime for the context.
     *
     */
    CorfuRuntime runtime;

    /** The type of context to build.
     *
     */
    TransactionType type = TransactionType.OPTIMISTIC;

    /** For snapshot transactions, the address the
     * snapshot will start at.
     */
    long snapshot = Address.NON_ADDRESS;

    public TransactionBuilder(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    /** Start the transaction with the parameters given
     * to the builder.
     */
    public void begin() {
        TransactionalContext.newContext(type.get.apply(this));
    }
}
