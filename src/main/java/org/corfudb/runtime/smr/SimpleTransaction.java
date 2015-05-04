package org.corfudb.runtime.smr;

import java.util.UUID;

/**
 * Created by mwei on 5/3/15.
 */
public class SimpleTransaction implements ITransaction {

    ITransactionCommand transaction;

    public SimpleTransaction()
    {

    }

    /**
     * Returns an SMR engine for a transactional context.
     *
     * @param streamID The streamID the SMR engine should run on.
     * @return The SMR engine to be used for a transactional context.
     */
    @Override
    public ISMREngine getEngine(UUID streamID) {
        return null;
    }

    /**
     * Set the command to be executed for this transaction.
     *
     * @param transaction The command(s) to be executed for this transaction.
     */
    @Override
    public void setTransaction(ITransactionCommand transaction) {
        this.transaction = transaction;
    }

    /**
     * Returns the transaction command.
     *
     * @return The command(s) to be executed for this transaction.
     */
    @Override
    public ITransactionCommand getTransaction() {
        return null;
    }
}
