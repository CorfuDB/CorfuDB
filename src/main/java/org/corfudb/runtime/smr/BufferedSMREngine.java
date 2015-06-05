package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 6/1/15.
 */
public class BufferedSMREngine<T> implements ISMREngine<T> {

    private final Logger log = LoggerFactory.getLogger(BufferedSMREngine.class);

    T underlyingObject;
    ITimestamp ts;
    UUID streamID;
    ICorfuDBInstance instance;

    ArrayList<ISMREngineCommand<T>> commandBuffer;

    class BufferedSMREngineOptions implements ISMREngineOptions
    {
        CompletableFuture<Object> returnResult;

        public BufferedSMREngineOptions(CompletableFuture<Object> returnResult)
        {
            this.returnResult = returnResult;
        }
        public CompletableFuture<Object> getReturnResult()
        {
            return this.returnResult;
        }
        public ICorfuDBInstance getInstance() { return instance; }

        @Override
        public UUID getEngineID() {
            return streamID;
        }
    }

    public BufferedSMREngine(T underlyingObject, ITimestamp ts, UUID streamID, ICorfuDBInstance instance)
    {
        this.underlyingObject = underlyingObject;
        this.ts = ts;
        this.streamID = streamID;
        this.instance = instance;
        this.commandBuffer = new ArrayList<ISMREngineCommand<T>>();
    }

    @SuppressWarnings("unchecked")
    public BufferedSMREngine(ITimestamp ts, UUID streamID, ICorfuDBInstance instance, Class<?> objClass, Class<?>... args)
    {
        try {
            this.ts = ts;
            this.streamID = streamID;
            this.instance = instance;

            underlyingObject = (T) objClass
                    .getConstructor(Arrays.stream(args)
                            .map(Class::getClass)
                            .toArray(Class[]::new))
                    .newInstance(args);

            this.commandBuffer = new ArrayList<ISMREngineCommand<T>>();

            ITimestamp streamPointer;
            IStream stream = instance.openStream(streamID);
            streamPointer = stream.getCurrentPosition();
            //one shot sync
            while (ts.compareTo(streamPointer) > 0) {
                IStreamEntry entry;
                try {
                   entry = stream.readNextEntry();
                } catch (HoleEncounteredException hee)
                {
                    // a hole might exist, but we shouldn't fill.
                    return;
                }
                try {
                    if (entry.getTimestamp().compareTo(ts) == 0) {
                        //don't read the sync point, since that contains
                        //the transaction...
                        streamPointer = stream.getCurrentPosition();
                        return;
                    }
                    if (entry instanceof ITransaction) {
                        ITransaction transaction = (ITransaction) entry;
                        transaction.executeTransaction(this);
                    }
                    else if (entry.getPayload() instanceof SMRLocalCommandWrapper)
                    {
                        //drop, we don't process local commands, and not only that, we must skip past the
                        //next entry.
                        SMRLocalCommandWrapper w = (SMRLocalCommandWrapper) entry.getPayload();
                        stream.seek(stream.getNextTimestamp(w.destination));
                    } else {
                        ISMREngineCommand<T> function = (ISMREngineCommand<T>) entry.getPayload();
                        function.accept(underlyingObject, new BufferedSMREngineOptions(new CompletableFuture<Object>()));
                    }
                } catch (Exception e) {
                    log.error("Exception executing buffered command at " + entry.getTimestamp() + " for stream ID " + stream.getStreamID(), e);
                    //ignore entries we don't know what to do about.
                }
                streamPointer = stream.getCurrentPosition();
            }
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
        this.underlyingObject = object;
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
        //Buffered SMR engines cannot be sync'd
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
        if (!readOnly)
        {
            //buffer the command.
            commandBuffer.add(command);
        }
        command.accept(underlyingObject, new BufferedSMREngineOptions(completion));
        return ts;
    }

    /**
     * Checkpoint the current state of the SMR engine.
     *
     * @return The timestamp the checkpoint was inserted at.
     */
    @Override
    public ITimestamp checkpoint() throws IOException {
        //buffered smr engines can't be checkpointed
        return null;
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
        return streamID;
    }
}
