package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

import static org.corfudb.generator.distributions.Keys.KeyId;
import static org.corfudb.generator.distributions.Streams.StreamId;

/**
 * Removes the value from a corfu table
 */
@Slf4j
public class RemoveOperation extends Operation {

    @Getter
    private final Operation.Context context;
    private final State.CorfuTablesGenerator tableManager;

    public RemoveOperation(State state, State.CorfuTablesGenerator tableManager) {
        super(state, Type.REMOVE);
        this.tableManager = tableManager;

        StreamId streamId = state.getStreams().sample();
        KeyId key = state.getKeys().sample();
        this.context = Context.builder()
                .streamId(streamId)
                .key(key)
                .build();
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            tableManager.getMap(context.getStreamId()).remove(context.getKey().getKey());

            String correctnessRecord = String.format(
                    "%s, %s:%s",
                    opType.getOpType(), context.getStreamId(), context.getKey().getKey()
            );
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            addToHistory();

            if (!TransactionalContext.isInTransaction()) {
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
