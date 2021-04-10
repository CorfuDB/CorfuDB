package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.RemoveOperation;
import org.corfudb.generator.operations.WriteOperation;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.State.CorfuTablesGenerator;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends AbstractTxOperation {
    public SnapshotTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager) {
        super(state, Operation.Type.TX_SNAPSHOT, operations, tablesManager);
    }

    @Override
    public void execute() {
        try {
            // Safety Hack for not having snapshot in the future
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_START);
            startSnapshotTx();

            int numOperations = state.getOperationCount().sample();
            List<Operation.Type> operationTypes = this.operations.sample(numOperations);

            EnumSet<Operation.Type> excludedOps = EnumSet.of(
                    Type.TX_OPTIMISTIC, Type.TX_SNAPSHOT, Type.REMOVE, Type.WRITE, Type.TX_NESTED
            );

            for (Operation.Type operationType : operationTypes) {
                if (excludedOps.contains(operationType)) {
                    continue;
                }

                operations.create(operationType).execute();
            }

            stopTx();
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_END);
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_ABORTED);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("No context data");
    }

    private void startSnapshotTx() {
        tablesManager.getRuntime().getObjectsView()
                .TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();
    }
}