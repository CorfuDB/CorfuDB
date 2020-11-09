package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;

/**
 * CorfuStoreShim is a thin layer over CorfuStore that provides certain metadata management
 * that carry business logic specific to verticals.
 *
 * Created by hisundar on 2020-09-16
 */
public class CorfuStoreShim extends CorfuStore {

    public CorfuStoreShim(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Start a transaction with snapshot isolation level at the latest available snapshot.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @return Returns a Transaction context.
     */
    @Nonnull
    @Override
    public TxnContextShim txn(@Nonnull final String namespace) {
        return this.txn(namespace, IsolationLevel.snapshot());
    }

    /**
     * Start appending mutations to a transaction.
     * The transaction does not begin until either a commit or the first read is invoked.
     * On read or commit the latest available snapshot will be used to resolve the transaction
     * unless the isolation level has a snapshot timestamp value specified.
     *
     * @param namespace Namespace of the tables involved in the transaction.
     * @param isolationLevel Snapshot (latest or specific) at which the transaction must execute.
     * @return Returns a transaction context instance.
     */
    @Nonnull
    @Override
    public TxnContextShim txn(@Nonnull final String namespace, IsolationLevel isolationLevel) {
        return new TxnContextShim(
                super.getRuntime().getObjectsView(),
                super.getRuntime().getTableRegistry(),
                namespace,
                isolationLevel);
    }
}
