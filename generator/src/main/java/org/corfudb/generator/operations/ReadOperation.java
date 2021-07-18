package org.corfudb.generator.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.KeysState.ThreadName;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.generator.util.StringIndexer;
import org.corfudb.runtime.collections.CorfuTable;

/**
 * Reads data from corfu table and saves the current state in the operation context
 */
@Slf4j
public class ReadOperation extends Operation {

    @Getter
    private final Context context;
    private final CorfuTablesGenerator tableManager;
    private final Correctness correctness;

    public ReadOperation(State state, CorfuTablesGenerator tableManager, Correctness correctness) {
        super(state, Type.READ);
        this.tableManager = tableManager;
        this.correctness = correctness;

        Keys.FullyQualifiedKey key = generateFqKey(state);

        this.context = Context.builder()
                .fqKey(key)
                .val(tableManager.get(key))
                .build();
    }

    @Override
    public void execute() {
        context.setVal(tableManager.get(context.getFqKey()));
        String logMessage = context.getCorrectnessRecord(opType);
        correctness.recordOperation(logMessage);

        // Accessing secondary objects
        CorfuTable<String, String> corfuMap = tableManager.getMap(context.getFqKey().getTableId());

        corfuMap.getByIndex(StringIndexer.BY_FIRST_CHAR, "a");
        corfuMap.getByIndex(StringIndexer.BY_VALUE, context.getVal());

        context.setVersion(tableManager.getVersion());

        if (!tableManager.isInTransaction()) {
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        }

        if (tableManager.isInTransaction()) {
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
        txContext.getSnapshotId().setVersion(context.getVersion());

        if (!context.getVersion().equals(txContext.getSnapshotId().getVersion())){
            throw new IllegalStateException("Inconsistent state");
        }
    }
}
