package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;

import java.util.Optional;
import java.util.UUID;

/**
 * Writes a new value to the corfu table
 */
@Slf4j
public class WriteOperation extends Operation {
    @Getter
    private final Context context;
    private final CorfuTablesGenerator tableManager;

    public WriteOperation(State state, CorfuTablesGenerator tableManager) {
        super(state, Operation.Type.WRITE);
        this.tableManager = tableManager;

        Keys.FullyQualifiedKey key = generateFqKey(state);

        this.context = Context.builder()
                .fqKey(key)
                .val(Optional.of(UUID.randomUUID().toString()))
                .build();
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        boolean transactional = tableManager.isInTransaction();
        if (transactional) {
            tableManager.put(context.getFqKey(), context.getVal().get());
            Correctness.recordOperation(context.getCorrectnessRecord(opType.getOpType()));
            state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
        }
    }
}
