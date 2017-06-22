package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.corfudb.runtime.CorfuRuntime;

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
    public CorfuRuntime runtime;

    /** The type of context to build.
     *
     */
    public TransactionType type = TransactionType.OPTIMISTIC;

    /** For snapshot transactions, the address the
     * snapshot will start at.
     */
    public long snapshot = -1L;

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
