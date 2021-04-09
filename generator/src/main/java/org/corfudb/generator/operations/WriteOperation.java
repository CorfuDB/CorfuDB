package org.corfudb.generator.operations;

import lombok.Getter;
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
    @Getter
    private final Context context;
    private final State.CorfuTablesGenerator tableManager;

    public WriteOperation(State state, State.CorfuTablesGenerator tableManager) {
        super(state, Operation.Type.WRITE);
        this.tableManager = tableManager;

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
            tableManager.getMap(context.getStreamId()).put(context.getKey().getKey(), context.getVal().get());

            Correctness.recordOperation(
                    context.getCorrectnessRecord(opType.getOpType()),
                    TransactionalContext.isInTransaction()
            );

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }
}
