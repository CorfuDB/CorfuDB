package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

/**
 * Removes the value from a corfu table
 */
@Slf4j
public class RemoveOperation extends Operation {

    @Getter
    private final Operation.Context context;
    private final CorfuTablesGenerator tableManager;

    public RemoveOperation(State state, CorfuTablesGenerator tableManager) {
        super(state, Type.REMOVE);
        this.tableManager = tableManager;

        Keys.FullyQualifiedKey key = generateFqKey(state);

        this.context = Context.builder()
                .fqKey(key)
                .build();
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (tableManager.isInTransaction()) {
            tableManager
                    .getMap(context.getFqKey().getTableId())
                    .remove(context.getFqKey().getKeyId().getKey());

            String correctnessRecord = String.format(
                    "%s, %s:%s",
                    opType.getOpType(), context.getFqKey().getTableId(), context.getFqKey().getKeyId().getKey()
            );
            Correctness.recordOperation(correctnessRecord);

            addToHistory();

            if (!tableManager.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }

    private void addToHistory() {
        KeysState.KeyEntry entry = KeysState.KeyEntry.builder()
                .version(context.getVersion())
                .value(Optional.empty())
                .threadId(KeysState.ThreadName.buildFromCurrentThread())
                .clientId("client")
                .build();

        state.getKeysState().put(context.getFqKey(), entry);
    }
}
