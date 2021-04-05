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

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class ReadOperation extends Operation {
    private final Context context;

    public ReadOperation(State state) {
        super(state, "Read");
        StreamId streamId = state.getStreams().sample();
        KeyId key = state.getKeys().sample();
        this.context = Context.builder()
                .streamId(streamId)
                .key(key)
                .val(state.getMap(streamId).get(key.getKey()))
                .build();
    }

    @Override
    public void execute() {
        Correctness.recordOperation(
                context.getCorrectnessRecord(shortName),
                TransactionalContext.isInTransaction()
        );

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
}
