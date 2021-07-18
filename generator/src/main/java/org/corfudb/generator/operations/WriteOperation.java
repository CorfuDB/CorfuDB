package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.KeysState.ThreadName;
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
    private final Correctness correctness;

    public WriteOperation(State state, CorfuTablesGenerator tableManager, Correctness correctness) {
        super(state, Operation.Type.WRITE);
        this.tableManager = tableManager;
        this.correctness = correctness;

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
            correctness.recordOperation(context.getCorrectnessRecord(opType));
            state.getCtx().updateLastSuccessfulWriteOperationTimestamp();

            addToHistory();
        } else {
            throw new IllegalStateException("Not supported operation");
        }
    }

    private void addToHistory() {
        Keys.Version stateVersion = state.getKeysState().getThreadLatestVersion(ThreadName.buildFromCurrentThread());
        context.setVersion(stateVersion);

        KeysState.SnapshotId snapshotId = KeysState.SnapshotId.builder()
                .clientId("client")
                .threadId(ThreadName.buildFromCurrentThread())
                .version(context.getVersion())
                .build();

        KeysState.KeyEntry entry = KeysState.KeyEntry.builder()
                .snapshotId(snapshotId)
                .value(context.getVal())
                .build();

        state.getKeysState().put(context.getFqKey(), entry);
    }
}
