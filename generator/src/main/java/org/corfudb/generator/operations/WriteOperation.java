package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;
import java.util.UUID;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class WriteOperation extends Operation {

    public WriteOperation(State state) {
        super(state, Operation.Type.WRITE);
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            StreamId streamId = state.getStreams().sample();
            KeyId key = state.getKeys().sample();
            String val = UUID.randomUUID().toString();

            state.getMap(streamId.getStreamId()).put(key.getKey(), val);

            OperationLogMessage message = Context.builder()
                    .fqKey(new Keys.FullyQualifiedKey(key, streamId))
                    .val(Optional.of(val))
                    .build()
                    .getCorrectnessRecord(opType);
            correctness.recordOperation(message);

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }
}
