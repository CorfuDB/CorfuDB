package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.UUID;

import static org.corfudb.generator.distributions.Keys.KeyId;
import static org.corfudb.generator.distributions.Streams.StreamId;

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
            StreamId streamId = state.getStreams().sample();
            KeyId key = state.getKeys().sample();
            String val = UUID.randomUUID().toString();

            state.getMap(streamId).put(key.getKey(), val);

            String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }
}
