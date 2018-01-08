package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;


/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class ReadOperation extends Operation {

    public ReadOperation(State state) {
        super(state);
        shortName = "Read";
    }

    @Override
    public void execute() {
        String streamId = (String) state.getStreams().sample(1).get(0);
        String key = (String) state.getKeys().sample(1).get(0);
        String val = state.getMap(CorfuRuntime.getStreamID(streamId)).get(key);


        String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
        Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

        // Accessing secondary objects
        ((CorfuTable)state.getMap((CorfuRuntime.getStreamID(streamId)))).
                getByIndex(State.StringIndexer.BY_FIRST_CHAR, "a");
        ((CorfuTable)state.getMap((CorfuRuntime.getStreamID(streamId)))).
                getByIndex(State.StringIndexer.BY_VALUE, val);

        if (!TransactionalContext.isInTransaction()) {
            state.setLastSuccessfulReadOperationTimestamp(System.currentTimeMillis());
        }

    }
}
