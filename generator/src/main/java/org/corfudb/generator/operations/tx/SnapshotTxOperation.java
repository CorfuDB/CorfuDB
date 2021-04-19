package org.corfudb.generator.operations.tx;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends AbstractTxOperation {

    @Getter
    private final List<Operation> nestedOperations;

    public SnapshotTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager,
                               Correctness correctness) {
        super(state, Operation.Type.TX_SNAPSHOT, operations, tablesManager, correctness);

        this.nestedOperations = createOperations();
    }

    @Override
    public void execute() {
        try {
            // Safety Hack for not having snapshot in the future
            correctness.recordTransactionMarkers(opType, TxStatus.START);
            startSnapshotTx();

            executeOperations();

            stopTx();
            correctness.recordTransactionMarkers(opType, TxStatus.END);
            state.getCtx().updateLastSuccessfulReadOperationTimestamp();
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxStatus.ABORTED);
        }
    }

    @Override
    protected List<Operation> createOperations() {
        List<Operation> subOperations = new ArrayList<>();

        int numOperations = state.getOperationCount().sample();
        List<Operation.Type> operationTypes = this.operations.sample(numOperations);

        EnumSet<Operation.Type> excludedOps = EnumSet.of(
                Type.TX_OPTIMISTIC, Type.TX_SNAPSHOT, Type.REMOVE, Type.WRITE, Type.TX_NESTED
        );

        for (Operation.Type operationType : operationTypes) {
            if (excludedOps.contains(operationType)) {
                continue;
            }

            subOperations.add(operations.create(operationType));
        }

        return subOperations;
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