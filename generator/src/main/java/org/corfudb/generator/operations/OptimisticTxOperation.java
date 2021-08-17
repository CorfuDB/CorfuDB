package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

import java.util.List;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class OptimisticTxOperation extends Operation {

    public OptimisticTxOperation(State state) {
        super(state, Type.TX_OPTIMISTIC);
    }

    @Override
    public void execute() {
        try {
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.START, false);
            long timestamp;
            state.startOptimisticTx();

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

            timestamp = state.stopTx();

            correctness.recordTransactionMarkers(opType, TxState.TxStatus.END, Keys.Version.build(timestamp), true);

            if (Address.isAddress(timestamp)) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }

        } catch (TransactionAbortedException tae) {
            // TX aborted because of conflict is a successful operation regarding
            // Liveness status.
            if (tae.getAbortCause() == AbortCause.CONFLICT) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
            correctness.recordTransactionMarkers(opType, TxState.TxStatus.ABORTED,  false);
        }
    }
}
