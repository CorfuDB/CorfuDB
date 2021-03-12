package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Streams;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.UUID;

import static org.corfudb.generator.distributions.Keys.*;
import static org.corfudb.generator.distributions.Streams.*;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class ReadOperation extends Operation {

    private final StreamName streamId;
    private final KeyId key;
    private final String val;

    public ReadOperation(State state) {
        super(state, "Read");

        streamId = state.getStreams().sample();
        key = state.getKeys().sample();
        val = state.getMap(streamId).get(key);
    }

    @Override
    public void execute() {
        String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
        Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = state.getMap(streamId);

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, val);

        if (!TransactionalContext.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }
    }

    @Override
    public void verify() {
        /**
         * key_state = state.get(self.map_id, self.key_id)
         *         key_at_version: int = key_state.get_at_version(self.version)
         *
         *         try:
         *             assert self.value == key_at_version
         *         except AssertionError:
         *             state.incorrect_read += 1
         *             inconsistency = ReadInconsistency(self, key_at_version, False, state, False)
         *             inconsistency.log_report()
         */
        state.getMap(streamId)
    }
}
