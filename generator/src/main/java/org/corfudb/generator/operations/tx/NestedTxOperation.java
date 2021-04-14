package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends AbstractTxOperation {

    private static final int MAX_NEST = 20;

    public NestedTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager,
                             Correctness correctness) {
        super(state, Operation.Type.TX_NESTED, operations, tablesManager, correctness);
    }

    @Override
    public void execute() {
        correctness.recordTransactionMarkers(opType, TxStatus.START);
        startOptimisticTx();

        int numNested = state.getOperationCount().sample();
        int nestedTxToStop = numNested;

        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            try {
                startOptimisticTx();
                executeOperations();
            } catch (TransactionAbortedException tae) {
                log.warn("Transaction Aborted", tae);
                nestedTxToStop--;
            }
        }

        for (int i = 0; i < nestedTxToStop; i++) {
            stopTx();
        }
        long timestamp;
        try {
            timestamp = stopTx();
            correctness.recordTransactionMarkers(opType, TxStatus.END, Keys.Version.build(timestamp));
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxStatus.ABORTED);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("NestedTx doesn't have operation context");
    }
}
