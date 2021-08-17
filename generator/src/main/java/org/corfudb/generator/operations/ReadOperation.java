package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.State;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class ReadOperation extends Operation {

    public ReadOperation(State state) {
        super(state, Type.READ);
    }

    @Override
    public void execute() {
        StreamId streamId = state.getStreams().sample();
        KeyId key = state.getKeys().sample();
        String val = state.getMap(streamId.getStreamId()).get(key.getKey());

        OperationLogMessage logMessage = Context.builder()
                .fqKey(new Keys.FullyQualifiedKey(key, streamId))
                .val(Optional.of(val))
                .build()
                .getCorrectnessRecord(opType);
        correctness.recordOperation(logMessage);

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = state.getMap(streamId.getStreamId());

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, val);

        if (!TransactionalContext.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }
    }
}
