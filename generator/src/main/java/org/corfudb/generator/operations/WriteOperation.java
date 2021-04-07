package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;
import java.util.UUID;

/**
 * Writes a new value to the corfu table
 */
@Slf4j
public class WriteOperation extends Operation {
    private final Context context;

    public WriteOperation(State state) {
        super(state, "Write");
        this.context = Context.builder()
                .streamId(state.getStreams().sample())
                .key(state.getKeys().sample())
                .val(Optional.of(UUID.randomUUID().toString()))
                .build();
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            state.getMap(context.getStreamId()).put(context.getKey().getKey(), context.getVal().get());

            Correctness.recordOperation(
                    context.getCorrectnessRecord(shortName),
                    TransactionalContext.isInTransaction()
            );

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }
}
