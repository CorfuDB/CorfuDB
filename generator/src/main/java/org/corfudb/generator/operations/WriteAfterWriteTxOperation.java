package org.corfudb.generator.operations;

import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by rmichoud on 10/6/17.
 */
public class WriteAfterWriteTxOperation extends Operation {

    public WriteAfterWriteTxOperation(State state) {
        super(state, Type.TX_WAW);
    }

    @Override
    public void execute() {
        correctness.recordTransactionMarkers(opType, TxState.TxStatus.START, false);
        long timestamp;
        state.startWriteAfterWriteTx();

        int numOperations = state.getOperationCount().sample();
        List<Operation> operations = state.getOperations().sample(numOperations);

        for (Operation operation : operations) {
            if (operation instanceof OptimisticTxOperation
                    || operation instanceof SnapshotTxOperation
                    || operation instanceof NestedTxOperation) {
                continue;
            }

            operation.execute();
        }
        try {
            timestamp = state.stopTx();
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.END, Keys.Version.build(timestamp), true);
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.ABORTED, false);
        }

    }
}
