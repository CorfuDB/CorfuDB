package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends Operation {

    private static final int MAX_NEST = 20;

    public NestedTxOperation(State state) {
        super(state);
        shortName = "TxNest";
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_START);
        state.startOptimisticTx();

        int numNested = state.getOperationCount().sample(1).get(0);
        int nestedTxToStop = numNested;
        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            try{
            state.startOptimisticTx();
            int numOperations = state.getOperationCount().sample(1).get(0);
            List<Operation> operations = state.getOperations().sample(numOperations);

            for (int x = 0; x < operations.size(); x++) {
                if (operations.get(x) instanceof OptimisticTxOperation ||
                        operations.get(x) instanceof SnapshotTxOperation
            || operations.get(x) instanceof NestedTxOperation) {
                    continue;
                }
                operations.get(x).execute();
            }
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
