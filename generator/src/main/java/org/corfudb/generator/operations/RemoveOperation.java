package org.corfudb.generator.operations;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

/**
 * Removes the value from a corfu table
 */
@Slf4j
public class RemoveOperation extends Operation {

    public RemoveOperation(State state) {
        super(state, Type.REMOVE);
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            StreamId streamId = state.getStreams().sample();
            KeyId key = state.getKeys().sample();
            state.getMap(streamId.getStreamId()).remove(key.getKey());

            correctness.recordOperation(new RemoveOperationLogMessage(new FullyQualifiedKey(key, streamId)));

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }

    @AllArgsConstructor
    public static class RemoveOperationLogMessage implements Correctness.LogMessage {
        private final Operation.Type opType = Type.REMOVE;
        private final FullyQualifiedKey fqKey;

        @Override
        public String getMessage() {
            return String.format("%s, %s:%s", opType.getOpType(), fqKey.getTableId(), fqKey.getKeyId().getKey());
        }
    }
}
