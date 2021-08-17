package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.List;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends Operation {
    public SnapshotTxOperation(State state) {
        super(state, Type.TX_SNAPSHOT);
    }

    @Override
    public void execute() {
        try {
            // Safety Hack for not having snapshot in the future
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.START, false);
            state.startSnapshotTx();

            int numOperations = state.getOperationCount().sample();
            List<Operation> operations = state.getOperations().sample(numOperations);

            for (Operation operation : operations) {
                if (operation instanceof OptimisticTxOperation
                        || operation instanceof SnapshotTxOperation
                        || operation instanceof RemoveOperation
                        || operation instanceof WriteOperation
                        || operation instanceof NestedTxOperation) {
                    continue;
                }

                operation.execute();
            }

            state.stopTx();
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.END, false);
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.ABORTED, false);
        }


    }
}