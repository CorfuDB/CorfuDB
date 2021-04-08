package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.KeysState.ThreadName;
import org.corfudb.generator.state.State;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

/**
 * Reads data from corfu table and saves the current state in the operation context
 */
@Slf4j
public class ReadOperation extends Operation {
    private final Context context;

    public ReadOperation(State state) {
        super(state, Type.READ);
        StreamId streamId = state.getStreams().sample();
        KeyId key = state.getKeys().sample();
        this.context = Context.builder()
                .streamId(streamId)
                .key(key)
                .val(Optional.ofNullable(state.getMap(streamId).get(key.getKey())))
                .build();
    }

    @Override
    public void execute() {
        String logMessage = context.getCorrectnessRecord(opType.getOpType());
        Correctness.recordOperation(logMessage, TransactionalContext.isInTransaction());

        if (TransactionalContext.isInTransaction()) {
            //transactional read
            addToHistoryTransactional();
        } else {

        }

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = state.getMap(context.getStreamId());

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, context.getVal());

        context.setVersion(Correctness.getVersion());

        addToHistory();

        if (!TransactionalContext.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }
    }

    private void addToHistory() {
        ThreadName currThreadName = ThreadName.buildFromCurrentThread();
        Keys.Version version = state.getKeysState().getThreadLatestVersion(currThreadName);
        context.setVersion(version);
    }

    private void addToHistoryTransactional() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
