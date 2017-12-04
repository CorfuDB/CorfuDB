package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.TransactionalContext;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {
    public RemoveOperation(State state) {
        super(state);
        shortName = "Rm";
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            String streamId = (String) state.getStreams().sample(1).get(0);
            String key = (String) state.getKeys().sample(1).get(0);
            state.getMap(CorfuRuntime.getStreamID(streamId)).remove(key);

            String correctnessRecord = String.format("%s, %s:%s", shortName, streamId, key);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.setLastSuccessfulWriteOperationTimestamp(System.currentTimeMillis());
            }
        }
    }
}
