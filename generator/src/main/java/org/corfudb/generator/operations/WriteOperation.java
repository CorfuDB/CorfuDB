package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class WriteOperation extends Operation {

    public WriteOperation(State state) {
        super(state, "Write");
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            String streamId = state.getStreams().sample();

            String key = state.getKeys().sample();
            String val = UUID.randomUUID().toString();
            state.getMap(CorfuRuntime.getStreamID(streamId)).put(key, val);

            String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }
}
