package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Created by mwei on 5/3/15.
 */
public interface ITransaction {

    /**
     * Returns an SMR engine for a transactional context.
     * @param streamID  The streamID the SMR engine should run on.
     * @return          The SMR engine to be used for a transactional context.
     */
    ISMREngine getEngine(UUID streamID);

    /**
     * Set the command to be executed for this transaction.
     * @param transaction   The command(s) to be executed for this transaction.
     */
    void setTransaction(ITransactionCommand transaction);

    /**
     * Returns the transaction command.
     * @return          The command(s) to be executed for this transaction.
     */
    ITransactionCommand getTransaction();
}

