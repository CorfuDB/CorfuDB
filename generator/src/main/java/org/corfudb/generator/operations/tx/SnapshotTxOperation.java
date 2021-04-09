package org.corfudb.generator.operations.tx;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.RemoveOperation;
import org.corfudb.generator.operations.WriteOperation;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends AbstractTxOperation {
    public SnapshotTxOperation(State state) {
        super(state, Operation.Type.TX_SNAPSHOT);
    }

    @Override
    public void execute() {
        try {
            // Safety Hack for not having snapshot in the future
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_START);
            startSnapshotTx();

            int numOperations = state.getOperationCount().sample();
            List<Operation> operations = state.getOperations().sample(numOperations);

            EnumSet<Operation.Type> excludedOps = EnumSet.of(
                    Type.TX_OPTIMISTIC, Type.TX_SNAPSHOT, Type.REMOVE, Type.WRITE, Type.TX_NESTED
            );

            for (Operation operation : operations) {
                if (excludedOps.contains(operation.getOpType())) {
                    continue;
                }

                operation.execute();
            }

            stopTx();
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_END);
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_ABORTED);
        }
    }

    private void startSnapshotTx() {
        state.getRuntime().getObjectsView()
                .TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();
    }
}