package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.entries.CorfuDBEntry;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created by mwei on 5/1/15.
 */
public class SimpleSMREngine<T> implements ISMREngine<T> {

    private final Logger log = LoggerFactory.getLogger(SimpleSMREngine.class);

    IStream stream;
    T underlyingObject;
    public ITimestamp streamPointer;
    ITimestamp lastProposal;
    Class<T> type;
    HashMap<ITimestamp, CompletableFuture> completionTable;
    HashSet<ITimestamp> localTable;

    class SimpleSMREngineOptions<Y extends T> implements ISMREngineOptions<Y>
    {
        public ICorfuDBInstance getInstance() { return stream.getInstance(); }

        @Override
        public UUID getEngineID() {
            return stream.getStreamID();
        }

        @Override
        public void setUnderlyingObject(Y object) {
            underlyingObject = object;
        }
    }

    public SimpleSMREngine(IStream stream, Class<T> type, Class<?>... args)
    {
        try {
            this.stream = stream;
            this.type = type;
            if (!ITimestamp.isMin(stream.getCurrentPosition()))
            {
                throw new RuntimeException(
                        "Attempt to start SMR engine on a stream which is not at the beginning (pos="
                                + stream.getCurrentPosition() + ")");
            }
            streamPointer = stream.getCurrentPosition();
            completionTable = new HashMap<ITimestamp, CompletableFuture>();
            localTable = new HashSet<ITimestamp>();

            underlyingObject = type
                    .getConstructor(Arrays.stream(args)
                            .map(Class::getClass)
                            .toArray(Class[]::new))
                    .newInstance(args);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
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
    @SuppressWarnings("unchecked")
    public <R> void sync(ITimestamp ts) {
        synchronized (this) {
            if (ts == null) {
                ts = stream.check();
                if (ts.compareTo(streamPointer) <= 0) {
                    //we've already read to the most recent position, no need to keep reading.
                    return;
                }
            }
            while (ts.compareTo(streamPointer) > 0) {
                IStreamEntry entry = null;
                try {
                    try (TransactionalContext tc =
                                 new TransactionalContext(this, ts, stream.getInstance(), PassthroughTransaction.class)) {
                        //for now, use this to pass the instance context to the deserializer.
                        entry = stream.readNextEntry();
                    }
                    if (entry == null)
                    {
                        // we've reached the end of this stream.
                        return;
                    }
                    // Add this, because now that we have next pointers, the pointer may jump beyond ts
                    if (entry.getTimestamp().compareTo(ts) > 0) return;
                    if (entry instanceof ITransaction)
                    {
                        ITransaction transaction = (ITransaction) entry;
                        transaction.setInstance(stream.getInstance());
                        transaction.executeTransaction(this);
                    }
                    else if (entry.getPayload() instanceof SMRLocalCommandWrapper)
                    {
                        if (localTable.contains(ts)) {
                            localTable.remove(ts);
                            SMRLocalCommandWrapper<T, R> function = (SMRLocalCommandWrapper<T, R>) entry.getPayload();
                            try (TransactionalContext tc =
                                         new TransactionalContext(this, entry.getTimestamp(), function.destination,
                                                 stream.getInstance(), LocalTransaction.class)) {
                                ITimestamp entryTS = entry.getTimestamp();
                                CompletableFuture<Object> completion = completionTable.getOrDefault(entryTS, null);
                                completionTable.remove(entryTS);
                                R result = function.command.apply(underlyingObject, new SimpleSMREngineOptions());
                                if (completion != null) {
                                    completion.complete(result);
                                }
                            }
                        }
                        else
                        {
                            log.debug("Dropping localTX proposed by other client@{}", ts);
                        }
                    }
                    else {
                        try (TransactionalContext tc =
                                     new TransactionalContext(this, entry.getTimestamp(), stream.getInstance(), PassthroughTransaction.class)) {
                            ISMREngineCommand<T, R> function = (ISMREngineCommand<T, R>) entry.getPayload();
                            ITimestamp entryTS = entry.getTimestamp();
                            CompletableFuture<R> completion = completionTable.getOrDefault(entryTS, null);
                            completionTable.remove(entryTS);
                            if (entry instanceof MultiCommand)
                            {
                                completion = new CompletableFuture<>();
                            }
                            // log.warn("syncing entry-" + entryTS + " cf=" + completion + (bStaleCompletion?" (stale)":""));
                            R result = (R) function.apply(underlyingObject, new SimpleSMREngineOptions());
                            if (completion != null) {
                                completion.complete(result);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("exception during sync@{}", stream.getCurrentPosition(), e);
                }
                if (entry != null)
                    streamPointer = entry.getTimestamp();
            }
        }
    }

    /**
     * Execute a read only command against this engine.
     *
     * @param command The command to execute. It must be read only.
     * @return The return value.
     */
    @Override
    public <R> R read(ISMREngineCommand<T, R> command) {
        return command.apply(underlyingObject, new SimpleSMREngineOptions<>());
    }

    /**
     * Propose a new command to the SMR engine.
     *
     * @param command       A lambda (BiConsumer) representing the command to be proposed.
     *                      The first argument of the lambda is the object the engine is acting on.
     *                      The second argument of the lambda contains some TX that the engine
     *                      The lambda must be serializable.
     *
     * @param completion    A completable future which will be fulfilled once the command is proposed,
     *                      which is to be completed by the command.
     *
     * @param readOnly      Whether or not the command is read only.
     *
     * @return              The timestamp the command was proposed at.
     */
    @Override
    public <R> ITimestamp propose(ISMREngineCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly) {
        if (readOnly)
        {
            R result = command.apply(underlyingObject, new SimpleSMREngineOptions<>());
            if (completion != null)
            {
                completion.complete(result);
            }
            return streamPointer;
        }
        try {
            ITimestamp t = stream.append(command);
            if (completion != null) { completionTable.put(t, completion); }
            lastProposal = t;
            return t;
        }
        catch (Exception e)
        {
            //well, propose is technically not reliable, so we can just silently drop
            //any exceptions.
            log.warn("Exception proposing new command!", e);
            return null;
        }
    }

    /**
     * Propose a local command to the SMR engine. A local command is one which is executed locally
     * only, but may propose other commands which affect multiple objects.
     *
     * @param command    A lambda representing the command to be proposed
     * @param completion A completion to be fulfilled.
     * @param readOnly   True, if the command is read only, false otherwise.
     * @return A timestamp representing the command proposal time.
     */
    @Override
    public <R> ITimestamp propose(ISMRLocalCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly) {
        if (readOnly) {
            R result = command.apply(underlyingObject, new SimpleSMREngineOptions());
            if (completion != null)
            {
                completion.complete(result);
            }
            return streamPointer;
        }
        try {
            ITimestamp[] t = stream.reserve(2);
            localTable.add(t[0]);
            if (completion != null) { completionTable.put(t[0], completion); }
            stream.write(t[0], new SMRLocalCommandWrapper<>(command, t[1]));
            lastProposal = t[0];
            return t[0];
        }
        catch (Exception e)
        {
            //well, propose is technically not reliable, so we can just silently drop
            //any exceptions.
            log.warn("Exception in local command propose...", e);
            return null;
        }
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
        SMRCheckpoint<T> checkpoint = new SMRCheckpoint<T>(streamPointer, underlyingObject);
        return stream.append(checkpoint);
    }

    /**
     * Get the timestamp of the most recently proposed command.
     *
     * @return A timestamp representing the most recently proposed command.
     */
    @Override
    public ITimestamp getLastProposal() {
        return lastProposal;
    }

    /**
     * Pass through to check for the underlying stream.
     *
     * @return A timestamp representing the most recently proposed command on a stream.
     */
    @Override
    public ITimestamp check() {
        return stream.check();
    }

    /**
     * Get the underlying stream ID.
     *
     * @return A UUID representing the ID for the underlying stream.
     */
    @Override
    public UUID getStreamID() {
        return stream.getStreamID();
    }

    /**
     * Get the CorfuDB instance that supports this SMR engine.
     *
     * @return A CorfuDB instance.
     */
    @Override
    public ICorfuDBInstance getInstance() {
        return stream.getInstance();
    }

    @Override
    public ITimestamp getStreamPointer() { return streamPointer; }
}
