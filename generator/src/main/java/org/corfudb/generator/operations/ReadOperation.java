package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Keys.KeyId;
import org.corfudb.generator.distributions.Streams.StreamId;
import org.corfudb.generator.state.KeysState.ThreadName;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.util.Optional;

/**
 * Reads data from corfu table and saves the current state in the operation context
 */
@Slf4j
public class ReadOperation extends Operation {
    @Getter
    private final Context context;
    private final State.CorfuTablesGenerator tableManager;
    private boolean keyFromTx;

    public ReadOperation(State state, State.CorfuTablesGenerator tableManager) {
        super(state, Type.READ);
        this.tableManager = tableManager;

        StreamId streamId = state.getStreams().sample();
        KeyId key = state.getKeys().sample();
        this.context = Context.builder()
                .streamId(streamId)
                .key(key)
                .val(Optional.ofNullable(tableManager.getMap(streamId).get(key.getKey())))
                .build();
    }

    @Override
    public void execute() {
        String logMessage = context.getCorrectnessRecord(opType.getOpType());
        Correctness.recordOperation(logMessage, TransactionalContext.isInTransaction());

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = tableManager.getMap(context.getStreamId());

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, context.getVal());

        context.setVersion(Correctness.getVersion());

        if (!TransactionalContext.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }

        if (TransactionalContext.isInTransaction()) {
            //transactional read
            addToHistoryTransactional();
        } else {
            addToHistory();
        }
    }

    private void addToHistory() {
        ThreadName currThreadName = ThreadName.buildFromCurrentThread();
        Keys.Version version = state.getKeysState().getThreadLatestVersion(currThreadName);
        context.setVersion(version);
    }

    private void addToHistoryTransactional() {
        TxState.TxContext txContext = state.getTransactions().get(ThreadName.buildFromCurrentThread());
        txContext.setVersion(context.getVersion());

        keyFromTx = txContext.contains(context.getFqKey());

        if (!context.getVersion().equals(txContext.getVersion())){
            throw new IllegalStateException("Inconsistent state");
        }
    }
}
