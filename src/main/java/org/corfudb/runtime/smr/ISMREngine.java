package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * An interface to an SMR engine
 * Created by mwei on 5/1/15.
 */
public interface ISMREngine<T> {

    interface ISMREngineOptions<T>
    {
        ICorfuDBInstance getInstance();
        UUID getEngineID();
        void setUnderlyingObject(T object);
    }

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     * @return          The object maintained by the SMR engine.
     */
    T getObject();

    /**
     * Set the underlying object. This method should ONLY be used by a TX engine to
     * restore state.
     */
    void setObject(T object);

    /**
     * Synchronize the SMR engine to a given timestamp, or pass null to synchronize
     * the SMR engine as far as possible.
     * @param ts        The timestamp to synchronize to, or null, to synchronize to the most
     *                  recent version.
     */
    <R> void sync(ITimestamp ts);

    /**
     * Execute a read only command against this engine.
     * @param command   The command to execute. It must be read only.
     * @param <R>       The return type of the command.
     * @return          The return value.
     */
    default <R> R read(ISMREngineCommand <T,R> command)
    {
        throw new UnsupportedOperationException("Read only commands NOT supported");
    }

    /**
     * Propose a new command to the SMR engine.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     *
     * @param completion    A completable future which will be fulfilled once the command is proposed,
     *                      which is to be completed by the command.
     *
     * @param readOnly      Whether or not the command is read only.
     *
     * @return              A timestamp representing the timestamp that the command was proposed to.
     */
     <R> ITimestamp propose(ISMREngineCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly);

     default <R> CompletableFuture<ITimestamp>
        proposeAsync(ISMREngineCommand<T,R> command, CompletableFuture<R> completion, boolean readOnly)
        {
            return CompletableFuture.completedFuture(propose(command, completion, readOnly));
        }
    /**
     * Propose a new command to the SMR engine.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     *
     * @param completion    A completable future which will be fulfilled once the command is proposed,
     *                      which is to be completed by the command.
     *
     * @return              A timestamp representing the timestamp that the command was proposed to.
     */
    default <R> ITimestamp propose(ISMREngineCommand<T, R> command, CompletableFuture<R> completion)
    {
        return propose(command, completion, false);
    }

    /**
     * Propose a new command to the SMR engine. This convenience function allows you to pass
     * the command without the completable future.
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     * @return              A timestamp representing the timestamp the command was proposed to.
     */
    default <R> ITimestamp propose(ISMREngineCommand<T, R> command)
    {
        return propose(command, null, false);
    }

    default <R> ITimestamp propose(ISMREngineCommand<T, R> command, boolean writeOnly) {
        return propose(command);
    }

    /**
     * Propose a local command to the SMR engine. A local command is one which is executed locally
     * only, but may propose other commands which affect multiple objects.
     * @param command       A lambda representing the command to be proposed
     * @param completion    A completion to be fulfilled.
     * @param readOnly      True, if the command is read only, false otherwise.
     * @return              A timestamp representing the command proposal time.
     */
    default <R> ITimestamp propose(ISMRLocalCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Propose a local command to the SMR engine. A local command is one which is executed locally
     * only, but may propose other commands which affect multiple objects.
     * @param command       A lambda representing the command to be proposed.
     * @param completion    A completion to be fulfilled.
     * @return              A timestamp representing the command proposal time.
     */
    default <R> ITimestamp propose(ISMRLocalCommand<T,R> command, CompletableFuture<R> completion)
    {
        return propose(command, completion, false);
    }

    /**
     * Checkpoint the current state of the SMR engine.
     * @return              The timestamp the checkpoint was inserted at.
     */
    ITimestamp checkpoint()
        throws IOException;

    /**
     * Get the timestamp of the most recently proposed command.
     * @return              A timestamp representing the most recently proposed command.
     */
    ITimestamp getLastProposal();

    /**
     * Pass through to check for the underlying stream.
     * @return              A timestamp representing the most recently proposed command on a stream.
     */
    ITimestamp check();

    /**
     * Get the underlying stream ID.
     * @return              A UUID representing the ID for the underlying stream.
     */
    UUID getStreamID();

    /**
     * Get the CorfuDB instance that supports this SMR engine.
     * @return              A CorfuDB instance.
     */
    default ICorfuDBInstance getInstance() {
        throw new UnsupportedOperationException("not yet implemented...");
    }

    /**
     * Get the timestamp of the entry the state machine most recently applied.
     * @return              The timestamp of the most recently synced entry.
     */
    default ITimestamp getStreamPointer() { throw new UnsupportedOperationException("not yet implemented"); }


}