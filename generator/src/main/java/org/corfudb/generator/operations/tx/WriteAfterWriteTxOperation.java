package org.corfudb.generator.operations.tx;

import lombok.Getter;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Keys;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.TxState.TxStatus;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.List;

/**
 * Write after write transaction operation
 */
public class WriteAfterWriteTxOperation extends AbstractTxOperation {

    @Getter
    private final List<Operation> nestedOperations;

    public WriteAfterWriteTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager,
                                      Correctness correctness) {
        super(state, Operation.Type.TX_WAW, operations, tablesManager, correctness);

        this.nestedOperations = createOperations();
    }

    @Override
    public void execute() {
        correctness.recordTransactionMarkers(opType, TxStatus.START);
        long timestamp;
        startWriteAfterWriteTx();

        executeOperations();

        try {
            timestamp = stopTx();
            correctness.recordTransactionMarkers(opType, TxStatus.END, Keys.Version.build(timestamp));
        } catch (TransactionAbortedException tae) {
            correctness.recordTransactionMarkers(opType, TxStatus.ABORTED);
        }
    }

    @Override
    public Context getContext() {
        throw new UnsupportedOperationException("No context data");
    }

    public void startWriteAfterWriteTx() {
        tablesManager.getRuntime().getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }
}
