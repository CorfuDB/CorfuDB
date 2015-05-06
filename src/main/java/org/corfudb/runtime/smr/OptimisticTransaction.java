package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 5/6/15.
 */
public class OptimisticTransaction implements ITransaction, IStreamEntry, Serializable {

    ITransactionCommand transaction;
    List<UUID> streamList;
    ITimestamp timestamp;
    transient CorfuDBRuntime runtime;
    transient ISMREngine executingEngine;

    class OptimisticTransactionOptions implements ITransactionOptions
    {

        public OptimisticTransactionOptions() {
        }

        @Override
        public CompletableFuture<Object> getReturnResult() {
            return null;
        }
    }

    public OptimisticTransaction(CorfuDBRuntime runtime)
    {
        streamList = null;
        this.runtime = runtime;
    }

    /**
     * Gets the list of of the streams this entry belongs to.
     *
     * @return The list of streams this entry belongs to.
     */
    @Override
    public List<UUID> getStreamIds() {
        return null;
    }

    /**
     * Returns whether this entry belongs to a given stream ID.
     *
     * @param stream The stream ID to check
     * @return True, if this entry belongs to that stream, false otherwise.
     */
    @Override
    public boolean containsStream(UUID stream) {
        return false;
    }

    /**
     * Gets the timestamp of the stream this entry belongs to.
     *
     * @return The timestamp of the stream this entry belongs to.
     */
    @Override
    public ITimestamp getTimestamp() {
        return null;
    }

    /**
     * Set the timestamp.
     *
     * @param ts
     */
    @Override
    public void setTimestamp(ITimestamp ts) {

    }

    /**
     * Gets the payload of this stream.
     *
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        return null;
    }

    /**
     * Returns an SMR engine for a transactional context.
     *
     * @param streamID The streamID the SMR engine should run on.
     * @param objClass The class that the SMR engine runs against.
     * @return The SMR engine to be used for a transactional context.
     */
    @Override
    public ISMREngine getEngine(UUID streamID, Class<?> objClass) {
        return null;
    }

    /**
     * Registers a stream to be part of a transactional context.
     *
     * @param stream A stream that will be joined into this transaction.
     */
    @Override
    public void registerStream(UUID stream) {

    }

    /**
     * Sets the CorfuDB runtime for this transaction. Used when deserializing
     * the transaction.
     *
     * @param runtime The runtime to use for this transaction.
     */
    @Override
    public void setCorfuDBRuntime(CorfuDBRuntime runtime) {

    }

    /**
     * Set the command to be executed for this transaction.
     *
     * @param transaction The command(s) to be executed for this transaction.
     */
    @Override
    public void setTransaction(ITransactionCommand transaction) {

    }

    /**
     * Execute this command on a specific SMR engine.
     *
     * @param engine The SMR engine to run this command on.
     */
    @Override
    public void executeTransaction(ISMREngine engine) {

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

    /**
     * Propose to the SMR engine(s) for the transaction to be executed.
     *
     * @return The timestamp that the transaction was proposed at.
     * This timestamp should be a valid timestamp for all streams
     * that the transaction belongs to, otherwise, the transaction
     * will abort.
     */
    @Override
    public ITimestamp propose() throws IOException {
        return null;
    }
}
