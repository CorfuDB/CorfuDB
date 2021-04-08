package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class OptimisticTxOperation extends AbstractTxOperation {

    public OptimisticTxOperation(State state) {
        super(state, Operation.Type.TX_OPTIMISTIC);
    }

    @Override
    public void execute() {
        try {
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_START);
            long timestamp;
            state.startOptimisticTx();

            executeOperations();

            timestamp = state.stopTx();

            Correctness.recordTransactionMarkers(true, opType.getOpType(), Correctness.TX_END,
                    Long.toString(timestamp));

            if (Address.isAddress(timestamp)) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }

        } catch (TransactionAbortedException tae) {
            // TX aborted because of conflict is a successful operation regarding
            // Liveness status.
            if (tae.getAbortCause() == AbortCause.CONFLICT) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_ABORTED);
        }
    }
}
