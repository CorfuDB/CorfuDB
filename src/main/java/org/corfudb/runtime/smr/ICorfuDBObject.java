package org.corfudb.runtime.smr;

import lombok.SneakyThrows;
import org.corfudb.runtime.objects.DynamicallyGeneratedException;
import org.corfudb.runtime.smr.smrprotocol.LambdaSMRCommand;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Created by mwei on 5/1/15.
 */
public interface ICorfuDBObject<U> extends Serializable {

    /**
     * Returns the SMR engine associated with this object.
     */
    @SuppressWarnings("unchecked")
    default ISMREngine<U> getSMREngine()
    {
        ITransaction tx = getUnderlyingTransaction();
        if (tx != null)
        {
            return tx.getEngine(getStreamID(), getUnderlyingType());
        }
        return getUnderlyingSMREngine();
    }

    /**
     * Get the type of the underlying object
     */
    default Class<U> getUnderlyingType() {
        throw new DynamicallyGeneratedException();
    }

    /**
     * Get the UUID of the underlying stream
     */
    default UUID getStreamID() {
        throw new DynamicallyGeneratedException();
    }

    /**
     * Get the in-memory state for this object.
     * @return  The in-memory state for this object.
     */
    default U getState() {
        throw new DynamicallyGeneratedException();
    }

    /** Get the current instance for this object.
     * @return  The current instance for this object.
     */
    default ICorfuDBInstance getInstance()
    {
        throw new DynamicallyGeneratedException();
    }

    /**
     * Get underlying SMR engine
     * @return  The SMR engine this object was instantiated under.
     */
    @SuppressWarnings("unchecked")
    default ISMREngine<U> getUnderlyingSMREngine() {
        return (ISMREngine<U>) getInstance().getBaseEngine(getStreamID(), getUnderlyingType(), this);
    }

    /**
     * Get the underlying transaction
     * @return  The transaction this object is currently participating in.
     */
    default ITransaction getUnderlyingTransaction()
    {
        return TransactionalContext.getTX();
    }

    /**
     * Must be called whenever the object is accessed, in order to ensure
     * that every write is read.
     */
    @SuppressWarnings("unchecked")
    default <R> R accessorHelper(ISMREngineCommand<U,R> command)
    {
        ISMREngine<U> e = getSMREngine();
        e.sync(null);
        return getSMREngine().read(command);
    }

    /**
     * Called whenever an object is to be mutated with the command that will
     * be executed.
     * @param command       The command to be executed.
     */
    @SuppressWarnings("unchecked")
    default void mutatorHelper(IConsumerOnlySMREngineCommand<U> command)
    {
        BiFunction<U, ISMREngine.ISMREngineOptions, Void> bf = (x,o) -> {
            command.apply(x,o);
            return null;
        };

        getSMREngine().propose(new LambdaSMRCommand<U, Void>(
                (BiFunction<U, ISMREngine.ISMREngineOptions, Void> & Serializable) bf), true);
    }

    /**
     * Called whenever and object will be both mutated and accessed.
     * @param command       The command to be executed.
     * @return              The result of the access.
     */
    @SuppressWarnings("unchecked")
    @SneakyThrows
    default <R> R mutatorAccessorHelper(ISMREngineCommand<U,R> command)
    {
        CompletableFuture<R> o = new CompletableFuture<>();
        ISMREngine<U> e = getSMREngine();
        e.proposeAsync(new LambdaSMRCommand<U, R>(command), o, false)
                .thenAccept(e::sync);
        return o.join();
    }

    /**
     * Called whenever a local command is to be proposed. A local command
     * is a command which is processed only at the local client, but
     * may generate results which insert commands into the log.
     * @param command       The local command to be executed.
     * @return              True, if the command succeeds. False otherwise.
     */
    default <R> R localCommandHelper(ISMRLocalCommand<U, R> command)
    {
        CompletableFuture<R> o = new CompletableFuture<R>();
        ITimestamp proposal = getSMREngine().propose(command, o);
        if (!isAutomaticallyPlayedBack()) {getSMREngine().sync(proposal);}
        return  o.join();
    }


    /**
     * Whether or not the object has been registered for automatic playback.
     * @return              True if the object is being automatically played back,
     *                      False otherwise.
     */
    default boolean isAutomaticallyPlayedBack()
    {
        return false;
    }

    /**
     * Manually generate a checkpoint and insert it into the log.
     * @return              The timestamp the checkpoint was generated and proposed.
     */
    default ITimestamp generateCheckpoint() {
        try {
            return getSMREngine().checkpoint();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error generating checkpoint.", e);
        }
    }

    /**
     * Triggered before an object is mutated.
     * @param timestamp     The timestamp of the state change triggering the handler.
     * @param object        The state of the object before the mutation.
     */
    default void preMutationHandler(ITimestamp timestamp, U object)
    {

    }

    /**
     * Triggered after an object is mutated.
     * @param timestamp     The timestamp of the object after triggering the handler
     * @param object        The state of the object after the mutation.
     */
    default void postMutationHandler(ITimestamp timestamp, U object)
    {

    }
}
