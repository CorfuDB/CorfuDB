package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends AbstractTxOperation {

    private static final int MAX_NEST = 20;

    public NestedTxOperation(State state) {
        super(state, "TxNest");
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_START);
        state.startOptimisticTx();

        int numNested = state.getOperationCount().sample();
        int nestedTxToStop = numNested;

        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            try {
                state.startOptimisticTx();
                executeOperations();
            } catch (TransactionAbortedException tae) {
                log.warn("Transaction Aborted", tae);
                nestedTxToStop--;
            }
        }

        for (int i = 0; i < nestedTxToStop; i++) {
            state.stopTx();
        }
        long timestamp;
        try {
            timestamp = state.stopTx();
            Correctness.recordTransactionMarkers(true, shortName, Correctness.TX_END,
                    Long.toString(timestamp));
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_ABORTED);
        }
    }
}
