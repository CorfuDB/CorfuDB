package org.corfudb.runtime.smr;

import org.corfudb.runtime.stream.ITimestamp;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 5/1/15.
 */
public interface ICorfuDBObject<T> {

    /**
     * Returns the SMR engine associated with this object.
     */
    ISMREngine getSMREngine();

    /**
     * Gets a transactional context for this object.
     * @return              A transactional context to be used during a transaction.
     */
    @SuppressWarnings("unchecked")
    default T getTransactionalContext(ITransaction tx)
    {
        try {
            return (T) this.getClass().getConstructor(this.getClass(), ISMREngine.class)
                    .newInstance(this, tx.getEngine(getSMREngine().getStreamID()));
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException | NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Must be called whenever the object is accessed, in order to ensure
     * that every write is read.
     */
    default void accessorHelper()
    {
        getSMREngine().sync(getSMREngine().check());
    }

    /**
     * Called whenever an object is to be mutated with the command that will
     * be executed.
     * @param command       The command to be executed.
     */
    @SuppressWarnings("unchecked")
    default void mutatorHelper(ISMREngineCommand command)
    {
        getSMREngine().propose(command);
    }

    /**
     * Called whenever and object will be both mutated and accessed.
     * @param command       The command to be executed.
     * @return              The result of the access.
     */
    @SuppressWarnings("unchecked")
    default Object mutatorAccessorHelper(ISMREngineCommand command)
    {
        CompletableFuture<Object> o = new CompletableFuture<Object>();
        ITimestamp proposal = getSMREngine().propose(command, o);
        if (!isAutomaticallyPlayedBack()) {getSMREngine().sync(proposal);}
        return o.join();
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
}
