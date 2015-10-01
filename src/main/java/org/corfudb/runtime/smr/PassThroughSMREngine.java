package org.corfudb.runtime.smr;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.smrprotocol.SMRCommand;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This SMR engine is designed for use for transactions only.
 * It passes through SMR operations directly to the underlying object,
 * bypassing the log.
 *
 * Also buffers commands, for collecting commands to write into the hint section of Logging Units.
 *
 * Created by mwei on 5/5/15.
 */
public class PassThroughSMREngine<T> implements ISMREngine<T>, IBufferedSMREngine<T> {

    class PassThroughSMREngineOptions<Y extends T> implements ISMREngineOptions<Y>
    {
        public ICorfuDBInstance getInstance() { return instance; }

        @Override
        public UUID getEngineID() {
            return streamID;
        }

        @Override
        public void setUnderlyingObject(Y object) {
            underlyingObject = object;
        }

    }

    T underlyingObject;
    ITimestamp ts;
    ICorfuDBInstance instance;
    UUID streamID;
    ArrayList<SMRCommand> commandBuffer;

    @Getter
    @Setter
    ICorfuDBObject implementingObject;

    public PassThroughSMREngine(T object, ITimestamp ts, ICorfuDBInstance instance, UUID streamID)
    {
        underlyingObject = object;
        this.ts = ts;
        this.instance = instance;
        this.streamID = streamID;
        this.commandBuffer = new ArrayList<SMRCommand>();
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
    public <R> void sync(ITimestamp ts) {
        // Always in sync.
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
         throw new UnsupportedOperationException("Checkpointing not supported!");
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
    public <R> ITimestamp propose(SMRCommand<T,R> command, CompletableFuture<R> completion, boolean readOnly) {
        R result = command.execute(underlyingObject, this);
        if (completion != null)
        {
            completion.complete(result);
        }
        commandBuffer.add(command);
        return ts;
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
        return null;
    }

    /**
     * Execute a read only command against this engine.
     *
     * @param command The command to execute. It must be read only.
     * @return The return value.
     */
    @Override
    public <R> R read(ISMREngineCommand<T, R> command) {
        return command.apply(underlyingObject, new PassThroughSMREngineOptions<>());
    }

    @Override
    public ArrayList<SMRCommand> getCommandBuffer() {
        return commandBuffer;
    }
}
