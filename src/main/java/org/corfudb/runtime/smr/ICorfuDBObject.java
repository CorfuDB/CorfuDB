package org.corfudb.runtime.smr;

import com.esotericsoftware.kryo.Kryo;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.IStreamingSequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.StreamingSequencer;
import org.corfudb.runtime.view.WriteOnceAddressSpace;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by mwei on 5/1/15.
 */
public interface ICorfuDBObject<U> extends Serializable {

    /**
     * Returns the SMR engine associated with this object.
     */
    @SuppressWarnings("unchecked")
    default ISMREngine getSMREngine()
    {
        ITransaction tx = getUnderlyingTransaction();
        if (tx != null)
        {
            return tx.getEngine(getStreamID(), getUnderlyingType());
        }

        //We need this until we implement custom serialization
        if (getUnderlyingSMREngine() == null)
        {
            CorfuDBRuntime cdr = CorfuDBRuntime.getRuntime("memory");
            IStream stream = cdr.getLocalInstance().openStream(getStreamID());
            SimpleSMREngine e = new SimpleSMREngine(stream, getUnderlyingType());
            e.sync(null);
            setUnderlyingSMREngine(e);
        }
        return getUnderlyingSMREngine();
    }

    /**
     * Get the type of the underlying object
     */
    default Class<U> getUnderlyingType() {
        for (Type t : this.getClass().getGenericInterfaces())
        {
            if (t instanceof ParameterizedType && ((ParameterizedType)t).getRawType().equals(ICorfuDBObject.class))
            {
                ParameterizedType p = (ParameterizedType) t;
                Type r = p.getActualTypeArguments()[0];
                if (r instanceof ParameterizedType)
                {
                    return (Class<U>) ((ParameterizedType)r).getRawType();
                }
                return (Class<U>) r;
            }
        }
        throw new RuntimeException("Couldn't resolve underlying type!");
    }

    /**
     * Get the UUID of the underlying stream
     */
    UUID getStreamID();

    /**
     * Get underlying SMR engine
     * @return  The SMR engine this object was instantiated under.
     */
    ISMREngine<U> getUnderlyingSMREngine();

    /**
     * Set underlying SMR engine
     * @param smrEngine The SMR engine to replace.
     */
    void setUnderlyingSMREngine(ISMREngine<U> engine);

    /**
     * Set the stream ID
     * @param streamID  The stream ID to set.
     */
    void setStreamID(UUID streamID);

    /**
     * Get the underlying transaction
     * @return  The transaction this object is currently participating in.
     */
    default ITransaction getUnderlyingTransaction()
    {
        return TransactionalContext.currentTX.get();
    }

    /**
     * Must be called whenever the object is accessed, in order to ensure
     * that every write is read.
     */
    @SuppressWarnings("unchecked")
    default <R> R accessorHelper(ISMREngineCommand<U,R> command)
    {
        CompletableFuture<R> o = new CompletableFuture<R>();
        getSMREngine().sync(getSMREngine().check());
        getSMREngine().propose(command, o, true);
        return o.join();
    }

    /**
     * Called whenever an object is to be mutated with the command that will
     * be executed.
     * @param command       The command to be executed.
     */
    @SuppressWarnings("unchecked")
    default <R> void mutatorHelper(ISMREngineCommand<U,R> command)
    {
        getSMREngine().propose(command);
    }

    /**
     * Called whenever and object will be both mutated and accessed.
     * @param command       The command to be executed.
     * @return              The result of the access.
     */
    @SuppressWarnings("unchecked")
    default <R> R mutatorAccessorHelper(ISMREngineCommand<U,R> command)
    {
        CompletableFuture<R> o = new CompletableFuture<R>();
        ITimestamp proposal = getSMREngine().propose(command, o);
        if (!isAutomaticallyPlayedBack()) {getSMREngine().sync(proposal);}
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
     * Handles upcalls, if implemented. When an SMR engine encounters
     * a upcall, it calls this handler. This default upcall handler
     * does nothing.
     * @param o             The object passed to the upcall handler.
     * @param cf            A completable future to be returned to the
     *                      accessor.
     */
    default void upcallHandler(Object o, CompletableFuture<Object> cf)
    {

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

    default ISMREngine instantiateSMREngine(IStream stream, Class<? extends ISMREngine> smrClass, Class<?>... args)
    {
        try {
            Class<?>[] c = smrClass.getConstructors()[0].getParameterTypes();

            return smrClass.getConstructor(IStream.class, Class.class, Class[].class)
                    .newInstance(stream, getUnderlyingType(), args);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }
}
