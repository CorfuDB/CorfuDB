package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.FullyQualifiedKey;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

import static org.corfudb.generator.distributions.Keys.KeyId;
import static org.corfudb.generator.distributions.Streams.StreamId;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class RemoveOperation extends Operation {
    private StreamId streamId;
    private KeyId key;
    private Keys.Version version;

    public RemoveOperation(State state) {
        super(state, "Rm");
    }

    @Override
    public void execute() {
        // Hack for Transaction writes only
        if (TransactionalContext.isInTransaction()) {
            streamId = state.getStreams().sample();
            key = state.getKeys().sample();
            state.getMap(streamId).remove(key.getKey());

            String correctnessRecord = String.format("%s, %s:%s", shortName, streamId, key);
            Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

            if (!TransactionalContext.isInTransaction()) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
        }
    }

    @Override
    public boolean verify() {
        throw new IllegalStateException("Not applicable");
    }

    @Override
    public void addToHistory() {
        FullyQualifiedKey fqKey = FullyQualifiedKey.builder().keyId(key).tableId(streamId).build();
        KeysState.KeyEntry entry = new KeysState.KeyEntry(
                version, Optional.empty(),
                KeysState.ThreadName.buildFromCurrentThread(), "client", Optional.empty()
        );
        state.getKeysState().put(fqKey, entry);
    }
}
