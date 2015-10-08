package org.corfudb.runtime.smr;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.smrprotocol.SMRCommand;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 10/6/15.
 */
@RequiredArgsConstructor
public class NullSMREngine implements ISMREngine<Void>{

    @Getter
    final ICorfuDBInstance instance;

    /**
     * Get the underlying object. The object is dynamically created by the SMR engine.
     *
     * @return The object maintained by the SMR engine.
     */
    @Override
    public Void getObject() {
        return null;
    }

    /**
     * Set the underlying object. This method should ONLY be used by a TX engine to
     * restore state.
     *
     * @param object
     */
    @Override
    public void setObject(Void object) {

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
        /* The null SMREngine always syncs to some timestamp position,
           to play back some specific entry (usually a transaction that
           has just been proposed)
         */
        CompletableFuture cf = TransactionalContext.getTransactionalFuture(ts);
        if (cf != null && cf.isDone()) { return; }
        else {
            IStreamEntry se = getInstance().getStreamAddressSpace().read(((SimpleTimestamp) ts).address);
            if (se.getPayload() instanceof SMRCommand) {
                SMRCommand c = (SMRCommand) se.getPayload();
                c.setInstance(instance);
                R result = (R) c.execute(null, this, ts);
                if (cf != null) {
                    cf.complete(result);
                }
            }
        }
    }

    @Override
    public void setImplementingObject(ICorfuDBObject object) {
    }

    @Override
    public ICorfuDBObject getImplementingObject() {
        return null;
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
    public <R> ITimestamp propose(SMRCommand<Void, R> command, CompletableFuture<R> completion, boolean readOnly) {
        return null;
    }

    /**
     * Checkpoint the current state of the SMR engine.
     *
     * @return The timestamp the checkpoint was inserted at.
     */
    @Override
    public ITimestamp checkpoint() throws IOException {
        return null;
    }

    /**
     * Get the timestamp of the most recently proposed command.
     *
     * @return A timestamp representing the most recently proposed command.
     */
    @Override
    public ITimestamp getLastProposal() {
        return null;
    }

    /**
     * Pass through to check for the underlying stream.
     *
     * @return A timestamp representing the most recently proposed command on a stream.
     */
    @Override
    public ITimestamp check() {
        return null;
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
}
