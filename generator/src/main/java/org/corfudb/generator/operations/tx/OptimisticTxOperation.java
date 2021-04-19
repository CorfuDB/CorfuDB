package org.corfudb.generator.operations.tx;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

import java.util.List;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class OptimisticTxOperation extends AbstractTxOperation {

    @Getter
    private final List<Operation> nestedOperations;

    public OptimisticTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager,
                                 Correctness correctness) {
        super(state, Operation.Type.TX_OPTIMISTIC, operations, tablesManager, correctness);

        this.nestedOperations = createOperations();
    }

    @Override
    public void execute() {
        try {
            correctness.recordTransactionMarkers(opType, TxStatus.START);
            long timestamp;
            startOptimisticTx();

            executeOperations();

            timestamp = stopTx();

            correctness.recordTransactionMarkers(opType, TxStatus.END, Keys.Version.build(timestamp));

            if (Address.isAddress(timestamp)) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }

        } catch (TransactionAbortedException tae) {
            // TX aborted because of conflict is a successful operation regarding
            // Liveness status.
            if (tae.getAbortCause() == AbortCause.CONFLICT) {
                state.getCtx().updateLastSuccessfulWriteOperationTimestamp();
            }
            correctness.recordTransactionMarkers(opType, TxStatus.ABORTED);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("No context data");
    }
}
