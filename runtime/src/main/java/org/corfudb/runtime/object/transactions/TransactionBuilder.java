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
    public TransactionType type = TransactionType.READ_AFTER_WRITE;

    /** For snapshot transactions, the address the
     * snapshot will start at.
     */
    public long snapshot = -1L;


    /** Require precise conflict detection.
     *
     * <p>If set to false, transaction conflict resolution
     * will rely on the sequencer, which may abort due
     * to hash collisions.
     *
     * <p>If set to true, transaction conflict resolution
     * will first attempt to resolve using the sequencer
     * and if the sequencer requests an abort, then we
     * will retry by reading the log manually. If a true
     * conflict exists, then we will abort, otherwise
     * we will reattempt to commit the transaction using
     * the manually resolved information. Setting
     * this value to true has significant performance
     * implications on an abort.
     */
    public boolean preciseConflicts = false;

    public TransactionBuilder(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    /** Start the transaction with the parameters given
     * to the builder.
     */
    public void begin() {
        Transactions.begin(type.get.apply(this));
    }
}
