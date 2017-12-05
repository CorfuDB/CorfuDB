package org.corfudb.generator.operations;

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.TransactionalContext;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class WriteOperation extends Operation {

    public WriteOperation(State state) {
        super(state);
        shortName = "Write";
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            String streamId = (String) state.getStreams().sample(1).get(0);

            String key = (String) state.getKeys().sample(1).get(0);
            String val = UUID.randomUUID().toString();
            state.getMap(CorfuRuntime.getStreamID(streamId)).put(key, val);

            String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.setLastSuccessfulWriteOperationTimestamp(System.currentTimeMillis());
            }
        }
    }
}
