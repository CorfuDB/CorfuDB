package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

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
     * @param objClass  The class that the SMR engine runs against.
     * @return          The SMR engine to be used for a transactional context.
     */
    ISMREngine getEngine(UUID streamID, Class<?> objClass);

    /**
     * Registers a stream to be part of a transactional context.
     * @param stream    A stream that will be joined into this transaction.
     */
    void registerStream(UUID stream);

    /**
     * Set the CorfuDB instance for this transaction. Used during deserialization.
     * @param instance  The CorfuDB instance used for this tx.
     */
    void setInstance(ICorfuDBInstance instance);

    /**
     * return a pointer to the runtime managing this transaction.
     * this is needed for transactions on objects which may create
     * new objects during that transaction.
     * @return the runtime
     */
    ICorfuDBInstance getInstance();

    /**
     * Set the command to be executed for this transaction.
     * @param transaction   The command(s) to be executed for this transaction.
     */
    <T> void setTransaction(ITransactionCommand<T> transaction);

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

    default void setTimestamp(ITimestamp timestamp)
    {
        throw new UnsupportedOperationException("unsupported op!");
    }

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

