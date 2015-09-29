package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This SMR engine is identical to the OneShot, except after a OneShot's one-time use,
 * this SMR engine can be reused, starting at its previously synced location. This is to enable
 * caching of SMR engines that are created during DeferredTxns.
 *
 * Write commands are reflected immediately on the object (for TX support)
 * and not written to the log. They are also buffered, so that the commands can be collected and inserted into the
 * hint section of the Logging Units.
 *
 * So you should NEVER, EVER, use this engine unless you are IMPLEMENTING
 * TRANSACTIONS.
 *
 * Created by amytai on 7/15/15.
 */
public class CachedSMREngine<T> implements ISMREngine<T>, IBufferedSMREngine<T> {

    IStream stream;
    T underlyingObject;
    ITimestamp streamPointer;
    ITimestamp syncPoint;
    ArrayList<ISMREngineCommand> commandBuffer;

    class CachedSMREngineOptions<Y extends T> implements ISMREngineOptions<Y> {

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

    public CachedSMREngine(IStream stream, Class<T> type, ITimestamp syncPoint, Class<?>... args)
    {
        try {
            this.stream = stream;
            streamPointer = stream.getCurrentPosition();

            underlyingObject = type
                    .getConstructor(Arrays.stream(args)
                            .map(Class::getClass)
                            .toArray(Class[]::new))
                    .newInstance(args);

            this.syncPoint = syncPoint;
            this.commandBuffer = new ArrayList<ISMREngineCommand>();
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
     * Sync the SMR engine until JUST BEFORE the sync point.
     *
     * @param ts The timestamp to synchronize to, or null, to synchronize to the most
     *           recent version.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> void sync(ITimestamp ts) {
        /*
        synchronized (this) {
            if (ts == null) ts = stream.getCurrentPosition();
            while (ts.compareTo(streamPointer) > 0) {
                try {
                    IStreamEntry entry = stream.readNextEntry();
                    if (entry.getTimestamp().compareTo(ts) > 0) return;
                    if (entry.getTimestamp().compareTo(ts) == 0)
                    {
                        //don't read the sync point, since that contains
                        //the transaction...
                        return;
                    }
                    if (entry instanceof ITransaction)
                    {
                        ITransaction transaction = (ITransaction) entry;
                        transaction.executeTransaction(this);
                    }
                    else {
                        ISMREngineCommand<T,R> function = (ISMREngineCommand<T,R>) entry.getPayload();
                        function.apply(underlyingObject, new CachedSMREngineOptions());
                    }
                } catch (Exception e) {
                    //ignore entries we don't know what to do about.
                }
                streamPointer = stream.getCurrentPosition();
            }
        }
        */

        try {
            stream.readToAsync(ts == null ? stream.checkAsync().get() : ts)
                    .thenAccept(x -> {
                        if (x != null) {
                            for (IStreamEntry entry : x) {
                                if (entry.getTimestamp().compareTo(ts) > 0) return;
                                if (entry.getTimestamp().compareTo(ts) == 0) {
                                    //don't read the sync point, since that contains
                                    //the transaction...
                                    return;
                                }
                                if (entry instanceof ITransaction) {
                                    ITransaction transaction = (ITransaction) entry;
                                    transaction.executeTransaction(this);
                                } else {
                                    ISMREngineCommand<T, R> function = (ISMREngineCommand<T, R>) entry.getPayload();
                                    function.apply(underlyingObject, new CachedSMREngineOptions());
                                }
                            }
                        }
                        streamPointer = stream.getCurrentPosition();
                    }).get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public <R> ITimestamp propose(ISMREngineCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly) {
            R result = command.apply(underlyingObject, new CachedSMREngineOptions<T>());
            if (completion != null)
            {
                completion.complete(result);
            }
            commandBuffer.add(command);
            return streamPointer;
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
        return streamPointer;
    }

    /**
     * Pass through to check for the underlying stream.
     *
     * @return A timestamp representing the most recently proposed command on a stream.
     */
    @Override
    public ITimestamp check() {
        return syncPoint;
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
     * Execute a read only command against this engine.
     *
     * @param command The command to execute. It must be read only.
     * @return The return value.
     */
    @Override
    public <R> R read(ISMREngineCommand<T, R> command) {
        return command.apply(underlyingObject, new CachedSMREngineOptions<>());
    }

    @Override
    public ITimestamp getStreamPointer() {
        return streamPointer;
    }

    @Override
    public ArrayList<ISMREngineCommand> getCommandBuffer() {
        return commandBuffer;
    }
}
