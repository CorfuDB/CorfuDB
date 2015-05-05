package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.function.Function;
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
     * Functional version of getEngine().
     * @return          A function which returns an SMR engine given a stream ID.
     */
    default Function<UUID, ISMREngine> getEngine()
    {
        return this::getEngine;
    }
    /**
     * Registers a stream to be part of a transactional context.
     * @param stream    A stream that will be joined into this transaction.
     */
    void registerStream(UUID stream);

    /**
     * Set the command to be executed for this transaction.
     * @param transaction   The command(s) to be executed for this transaction.
     */
    void setTransaction(ITransactionCommand transaction);

    /**
     * Execute this command on a specific SMR engine.
     * @param engine        The SMR engine to run this command on.
     */
    void executeTransaction(ISMREngine engine);

    /**
     * Returns the transaction command.
     * @return          The command(s) to be executed for this transaction.
     */
    ITransactionCommand getTransaction();

    /**
     * Propose to the SMR engine(s) for the transaction to be executed.
     * @return          The timestamp that the transaction was proposed at.
     *                  This timestamp should be a valid timestamp for all streams
     *                  that the transaction belongs to, otherwise, the transaction
     *                  will abort.
     */
    ITimestamp propose()
    throws IOException;
}

