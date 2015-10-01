package org.corfudb.runtime.smr;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.smrprotocol.SMRCommand;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This is a special SMR engine that syncs to a given timestamp ONCE
 * and stays locked there.
 *
 * Write commands are reflected immediately on the object (for TX support)
 * and not written to the log.
 *
 * So you should NEVER, EVER, use this engine unless you are IMPLEMENTING
 * TRANSACTIONS.
 *
 * Created by mwei on 5/5/15.
 */
public class OneShotSMREngine<T> implements ISMREngine<T> {

    IStream stream;
    T underlyingObject;
    ITimestamp streamPointer;
    ITimestamp syncPoint;

    @Getter
    @Setter
    ICorfuDBObject implementingObject;

    class OneShotSMREngineOptions<Y extends T> implements ISMREngineOptions<Y> {

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

    public OneShotSMREngine(IStream stream, Class<T> type, ITimestamp syncPoint, Class<?>... args)
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
        synchronized (this) {
            ts = syncPoint;
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
                        function.apply(underlyingObject, new OneShotSMREngineOptions());
                    }
                } catch (Exception e) {
                    //ignore entries we don't know what to do about.
                }
                streamPointer = stream.getCurrentPosition();
            }
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
    public <R> ITimestamp propose(SMRCommand<T, R> command, CompletableFuture<R> completion, boolean readOnly) {
            R result = command.execute(underlyingObject, this);
            if (completion != null)
            {
                completion.complete(result);
            }
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
        return command.apply(underlyingObject, new OneShotSMREngineOptions<>());
    }

    @Override
    public ITimestamp getStreamPointer() {
        return streamPointer;
    }

}
