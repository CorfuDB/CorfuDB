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

/**
 * Removes the value from a corfu table
 */
@Slf4j
public class RemoveOperation extends Operation {

    @Getter
    private final Operation.Context context;
    private final CorfuTablesGenerator tableManager;
    private final Correctness correctness;

    public RemoveOperation(State state, CorfuTablesGenerator tableManager, Correctness correctness) {
        super(state, Type.REMOVE);
        this.tableManager = tableManager;
        this.correctness = correctness;

        Keys.FullyQualifiedKey key = generateFqKey(state);

        this.context = Context.builder()
                .fqKey(key)
                .build();
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (tableManager.isInTransaction()) {
            Keys.Version latestVersion = state
                    .getKeysState()
                    .getThreadLatestVersion(ThreadName.buildFromCurrentThread());

            context.setVersion(latestVersion);

            tableManager
                    .getMap(context.getFqKey().getTableId())
                    .remove(context.getFqKey().getKeyId().getKey());

            String correctnessRecord = String.format(
                    "%s, %s:%s",
                    opType.getOpType(), context.getFqKey().getTableId(), context.getFqKey().getKeyId().getKey()
            );
            correctness.recordOperation(correctnessRecord);

            addToHistory();

            if (!tableManager.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }

    private void addToHistory() {
        KeysState.SnapshotId snapshotId = KeysState.SnapshotId.builder()
                .version(context.getVersion())
                .threadId(ThreadName.buildFromCurrentThread())
                .clientId("client")
                .build();

        KeysState.KeyEntry entry = KeysState.KeyEntry.builder()
                .snapshotId(snapshotId)
                .value(Optional.empty())
                .build();

        state.getKeysState().put(context.getFqKey(), entry);
    }
}
