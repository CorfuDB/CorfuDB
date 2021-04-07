package org.corfudb.generator.operations;

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

    private final Operation.Context context;

    public RemoveOperation(State state) {
        super(state, "Rm");

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
            state.getMap(context.getStreamId()).remove(context.getKey().getKey());

            String correctnessRecord = String.format(
                    "%s, %s:%s",
                    shortName, context.getStreamId(), context.getKey().getKey()
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
