package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.KeysState;
import org.corfudb.generator.state.KeysState.VersionedKey;
import org.corfudb.generator.state.State;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import static org.corfudb.generator.distributions.Keys.*;
import static org.corfudb.generator.distributions.Streams.*;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class ReadOperation extends Operation {

    private final StreamId streamId;
    private final KeyId key;
    private final String val;
    private Keys.Version version;

    public ReadOperation(State state) {
        super(state, "Read");

        streamId = state.getStreams().sample();
        key = state.getKeys().sample();
        val = state.getMap(streamId).get(key.getKey());
    }

    @Override
    public void execute() {
        String correctnessRecord = String.format("%s, %s:%s=%s", shortName, streamId, key, val);
        Correctness.recordOperation(correctnessRecord, TransactionalContext.isInTransaction());

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = state.getMap(streamId);

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, val);

        version = Correctness.getVersion();

        if (!TransactionalContext.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }
    }

    @Override
    public boolean verify() {
        FullyQualifiedKey fqKey = FullyQualifiedKey.builder().keyId(key).tableId(streamId).build();
        VersionedKey keyState = state.getKey(fqKey);

        return val.equals(keyState.get(version).getValue());
    }

    @Override
    public void addToHistory() {
        KeysState.ThreadName currThreadName = KeysState.ThreadName.buildFromCurrentThread();
        version = state.getKeysState().getThreadLatestVersion(currThreadName);
    }
}
