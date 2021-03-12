package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import static org.corfudb.generator.distributions.Keys.KeyId;
import static org.corfudb.generator.distributions.Streams.StreamName;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {

    public RemoveOperation(State state) {
        super(state, "Rm");
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            StreamName streamId = state.getStreams().sample();
            KeyId key = state.getKeys().sample();
            state.getMap(streamId).remove(key.getKey());

            String correctnessRecord = String.format("%s, %s:%s", shortName, streamId, key);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }

    @Override
    public void verify() {
        ???
    }
}
