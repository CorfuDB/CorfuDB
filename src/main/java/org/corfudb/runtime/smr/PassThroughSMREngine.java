package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This SMR engine is designed for use for transactions only.
 * It passes through SMR operations directly to the underlying object,
 * bypassing the log.
 *
 * Created by mwei on 5/5/15.
 */
public class PassThroughSMREngine<T> implements ISMREngine<T> {

    class PassThroughSMREngineOptions implements ISMREngineOptions
    {
        CompletableFuture<Object> returnResult;

        public PassThroughSMREngineOptions(CompletableFuture<Object> returnResult)
        {
            this.returnResult = returnResult;
        }
        public CompletableFuture<Object> getReturnResult()
        {
            return this.returnResult;
        }
        public CorfuDBRuntime getRuntime() { return null; }

        @Override
        public UUID getEngineID() {
            return null;
        }

    }

    T underlyingObject;
    ITimestamp ts;

    public PassThroughSMREngine(T object, ITimestamp ts)
    {
        underlyingObject = object;
        this.ts = ts;
    }

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     *
     * @return The object maintained by the SMR engine.
     */
    @Override
    public T getObject() {
        return underlyingObject;
    }

    /**
     * Set the underlying object. This method should ONLY be used by a TX engine to
     * restore state.
     *
     * @param object
     */
    @Override
    public void setObject(T object) {
        underlyingObject = object;
    }

    /**
     * Synchronize the SMR engine to a given timestamp, or pass null to synchronize
     * the SMR engine as far as possible.
     *
     * @param ts The timestamp to synchronize to, or null, to synchronize to the most
     *           recent version.
     */
    @Override
    public void sync(ITimestamp ts) {
        // Always in sync.
    }

    /**
     * Checkpoint the current state of the SMR engine.
     *
     * @return The timestamp the checkpoint was inserted at.
     */
    @Override
    public ITimestamp checkpoint()
            throws IOException
    {
         throw new UnsupportedOperationException("Checkpointing not supported!");
    }

    /**
     * Propose a new command to the SMR engine.
     *
     * @param command    A lambda (BiConsumer) representing the command to be proposed.
     *                   The first argument of the lambda is the object the engine is acting on.
     *                   The second argument of the lambda contains some TX that the engine
     * @param completion A completable future which will be fulfilled once the command is proposed,
     *                   which is to be completed by the command.
     * @param readOnly   Whether or not the command is read only.
     * @return A timestamp representing the timestamp that the command was proposed to.
     */
    @Override
    public ITimestamp propose(ISMREngineCommand<T> command, CompletableFuture<Object> completion, boolean readOnly) {
        command.accept(underlyingObject, new PassThroughSMREngineOptions(completion));
        return ts;
    }

    /**
     * Get the timestamp of the most recently proposed command.
     *
     * @return A timestamp representing the most recently proposed command.
     */
    @Override
    public ITimestamp getLastProposal() {
        return ts;
    }

    /**
     * Pass through to check for the underlying stream.
     *
     * @return A timestamp representing the most recently proposed command on a stream.
     */
    @Override
    public ITimestamp check() {
        return ts;
    }

    /**
     * Get the underlying stream ID.
     *
     * @return A UUID representing the ID for the underlying stream.
     */
    @Override
    public UUID getStreamID() {
        return null;
    }
}
