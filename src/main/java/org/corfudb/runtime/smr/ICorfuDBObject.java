package org.corfudb.runtime.smr;

import com.esotericsoftware.kryo.Kryo;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.*;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by mwei on 5/1/15.
 */
public interface ICorfuDBObject<U> extends Serializable {

    Map<ICorfuDBObject, ICorfuDBInstance> instanceMap = Collections.synchronizedMap(new WeakHashMap<>());
    Map<ICorfuDBObject, ISMREngine> engineMap = Collections.synchronizedMap(new WeakHashMap<>());
    Map<ICorfuDBObject, UUID> uuidMap = Collections.synchronizedMap(new WeakHashMap<>());

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

        //We need this until we implement custom serialization
        if (getUnderlyingSMREngine() == null)
        {
            IStream stream = getInstance().openStream(getStreamID());
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
    default UUID getStreamID() {
        return uuidMap.get(this);
    }

    /**
     * Set the stream ID
     *
     * @param streamID The stream ID to set.
     */
    default void setStreamID(UUID streamID) {
        uuidMap.put(this, streamID);
    }

    /**
     * Get underlying SMR engine
     * @return  The SMR engine this object was instantiated under.
     */
    @SuppressWarnings("unchecked")
    default ISMREngine<U> getUnderlyingSMREngine() {
        return (ISMREngine<U>) engineMap.get(this);
    }

    /**
     * Set underlying SMR engine
     * @param smrEngine The SMR engine to replace.
     */
    @SuppressWarnings("unchecked")
    default void setUnderlyingSMREngine(ISMREngine<U> engine) {
        engineMap.put(this, engine);
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
        getSMREngine().propose(command, true);
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
        ISMREngine<U> e = getSMREngine();
        ITimestamp proposal = e.propose(command, o);
        if (!isAutomaticallyPlayedBack()) {e.sync(proposal);}
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

    default void setInstance(ICorfuDBInstance instance)
    {
        instanceMap.put(this, instance);
    }

    default ICorfuDBInstance getInstance()
    {
        return instanceMap.get(this);
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
