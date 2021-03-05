package org.corfudb.generator.operations;

import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by rmichoud on 10/6/17.
 */
public class WriteAfterWriteTxOperation extends Operation {

    public WriteAfterWriteTxOperation(State state) {
        super(state, "TxWaw");
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_START);
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
            Correctness.recordTransactionMarkers(true, shortName, Correctness.TX_END,
                    Long.toString(timestamp));
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_ABORTED);
        }

    }
}
