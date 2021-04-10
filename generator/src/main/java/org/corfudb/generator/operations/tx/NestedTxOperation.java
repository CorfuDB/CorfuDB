package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.State.CorfuTablesGenerator;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends AbstractTxOperation {

    private static final int MAX_NEST = 20;

    public NestedTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager) {
        super(state, Operation.Type.TX_NESTED, operations, tablesManager);
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_START);
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
            Correctness.recordTransactionMarkers(true, opType.getOpType(), Correctness.TX_END,
                    Long.toString(timestamp));
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_ABORTED);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("NestedTx doesn't have operation context");
    }
}
