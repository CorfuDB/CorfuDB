package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by rmichoud on 7/26/17.
 */
@Slf4j
public class NestedTxOperation extends Operation {

    private static final int MAX_NEST = 20;

    public NestedTxOperation(State state) {
        super(state, Type.TX_NESTED);
    }

    @Override
    public void execute() {
        correctness.recordTransactionMarkers(opType, TxState.TxStatus.START, false);
        state.startOptimisticTx();

        int numNested = state.getOperationCount().sample();
        int nestedTxToStop = numNested;

        for (int i = 0; i < numNested && i < MAX_NEST; i++) {
            try {
                state.startOptimisticTx();
                int numOperations = state.getOperationCount().sample();
                List<Operation> operations = state.getOperations().sample(numOperations);

                for (Operation operation : operations) {
                    if (operation instanceof OptimisticTxOperation ||
                            operation instanceof SnapshotTxOperation
                            || operation instanceof NestedTxOperation) {
                        continue;
                    }
                    operation.execute();
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
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.END, Keys.Version.build(timestamp), true);
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.ABORTED, false);
        }
    }
}
