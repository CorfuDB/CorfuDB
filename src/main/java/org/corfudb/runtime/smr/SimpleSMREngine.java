package org.corfudb.runtime.smr;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.entries.CorfuDBEntry;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.HoleFillingPolicy.IHoleFillingPolicy;
import org.corfudb.runtime.smr.HoleFillingPolicy.TimeoutHoleFillPolicy;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.Timestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    final ConcurrentHashMap<ITimestamp, CompletableFuture> completionTable = new ConcurrentHashMap<ITimestamp, CompletableFuture>();
    HashSet<ITimestamp> localTable;
    Map<UUID, IBufferedSMREngine> cachedEngines = Collections.synchronizedMap(new WeakHashMap<>());

    @Setter
    @Getter
    transient IHoleFillingPolicy holePolicy = new TimeoutHoleFillPolicy();

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

    public volatile ITimestamp lastApplied = ITimestamp.getMinTimestamp();
    public PriorityQueue<IStreamEntry> applyQueue = new PriorityQueue<>();

    public <R> void apply(IStreamEntry entry)
    {
        if (entry.getPayload() != null) {
            if (entry.getPayload() instanceof ITransaction)
            {
                ITransaction transaction = (ITransaction) entry.getPayload();
                transaction.setTimestamp(entry.getTimestamp());
                transaction.setInstance(stream.getInstance());
                transaction.executeTransaction(this);
            }
            else {
                try (TransactionalContext tc =
                             new TransactionalContext(this, entry.getTimestamp(), stream.getInstance(), PassthroughTransaction.class)) {
                  //  log.info("Applying command @ {} of type {}", entry.getTimestamp(), entry.getPayload().getClass());
                    ISMREngineCommand<T, R> function = (ISMREngineCommand<T, R>) entry.getPayload();
                    ITimestamp entryTS = entry.getTimestamp();
                    CompletableFuture<R> completion = completionTable.get(entryTS);
                /* commenting this out because this is to be expected with multiple clients */
                /*
                if (completion == null) {
                    log.debug("Completion @ {} is null", entryTS);
                }
                */
                    completionTable.remove(entryTS);
                    if (entry instanceof MultiCommand) {
                        completion = new CompletableFuture<>();
                    }
                    // log.warn("syncing entry-" + entryTS + " cf=" + completion + (bStaleCompletion?" (stale)":""));
                    R result = (R) function.apply(underlyingObject, new SimpleSMREngineOptions());
                    if (completion != null) {
                        completion.complete(result);
                    }
                }
            }
        }
        lastApplied = entry.getLogicalTimestamp();
    }

    public synchronized void learnAndApply(IStreamEntry entry)
    {
      //  log.info("learnApply id={} entry={} lastApplied={} count={} head={}", getStreamID(), entry.getTimestamp(), lastApplied, applyQueue.size(), applyQueue.peek() == null ? "null" : applyQueue.peek().getTimestamp());


        if(ITimestamp.isMin(lastApplied) && stream.getNextTimestamp(lastApplied).equals(entry.getLogicalTimestamp()))
        {
            apply(entry);
        }
        else
        {
            applyQueue.offer(entry);
        }

        while (applyQueue.peek() != null && applyQueue.peek().getLogicalTimestamp().equals(stream.getNextTimestamp(lastApplied)))
        {
            apply(applyQueue.poll());
        }

      //  log.info("learnApply id={} entry={} lastApplied={} count={} head={}", getStreamID(), entry.getLogicalTimestamp(), lastApplied, applyQueue.size(), applyQueue.peek() == null ? "null" : applyQueue.peek().getLogicalTimestamp());


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

        if (ts == null) {
            stream.checkAsync()
                    .thenAccept(t -> {
                        stream.readToAsync(t).thenAccept(entryArray -> {
                                    if (entryArray != null) {
                                        Arrays.stream(entryArray)
                                                .forEach(this::learnAndApply);
                                    }
                                }
                        ).join();
                    }).join();
        }
        else
        {
            stream.readToAsync(stream.getNextTimestamp(ts)).thenAccept(entryArray -> {
                        if (entryArray != null) {
                            Arrays.stream(entryArray)
                                    .forEach(this::learnAndApply);
                        }
                    }
            ).join();
        }
        /*
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
                                CompletableFuture<R> completion = completionTable.get(entryTS);
                                if (completion == null) {
                                    log.debug("Completion @ {} is null", entryTS);
                                }
                                completionTable.remove(entryTS);
                                if (entry instanceof MultiCommand) {
                                    completion = new CompletableFuture<>();
                                }
                                // log.warn("syncing entry-" + entryTS + " cf=" + completion + (bStaleCompletion?" (stale)":""));
                                R result = (R) function.apply(underlyingObject, new SimpleSMREngineOptions());
                                if (completion != null) {
                                    completion.complete(result);
                                }
                        }
                    }
                }
                catch (HoleEncounteredException hle)
                {
                    log.debug("hole encountered, applying policy during sync to {} @ {}", stream.getCurrentPosition(), ts);
                    holePolicy.apply(hle, stream);
                }
                catch (Exception e) {
                    log.error("exception during sync@{}", stream.getCurrentPosition(), e);
                }
                if (entry != null)
                    streamPointer = entry.getTimestamp();
            }
            */
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
            //ITimestamp t = stream.append(command);
            //if (completion != null) { completionTable.put(t, completion); }
            ITimestamp t = stream.reserve(1)[0];
                if (completion != null) {
                    completionTable.put(t, completion);
                }
            stream.write(t, command);
            lastProposal = t; //TODO: fix thread safety?
            return t;
        }
        catch (OverwriteException oe)
        {
            log.warn("Warning, propose resulted in overwrite @ {}, reproposing.", oe.address);
            return propose(command, completion, readOnly);
        }
        catch (Exception e)
        {
            log.warn("Exception proposing new command!", e);
            return null;
        }
    }

    @Override
    public <R> CompletableFuture<ITimestamp> proposeAsync(ISMREngineCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly) {
        if (readOnly)
        {
            /* TODO: pretty sure we need some kind of locking here (what if the object changes during a read?) */
            R result = command.apply(underlyingObject, new SimpleSMREngineOptions<>());
            if (completion != null)
            {
                completion.complete(result);
            }
            return CompletableFuture.completedFuture(streamPointer);
        }

        return stream.reserveAsync(1)
                .thenApplyAsync(
                  t -> {
                      if (completion != null) {
                          completionTable.put(t[0], completion);
                      }
                      try {
                          stream.write(t[0], command);
                      }  catch (Exception e)
                      {
                          //switch to sync operation.
                          return propose(command, completion, readOnly);
                      }
                      lastProposal = t[0]; //TODO: fix thread safety?
                      return t[0];
                  }
                );
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

    public Map<UUID, IBufferedSMREngine> getCachedEngines() {
        return cachedEngines;
    }

    public void addCachedEngine(UUID stream, IBufferedSMREngine engine) {
        cachedEngines.put(stream, engine);
    }
}
