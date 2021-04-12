package org.corfudb.generator.operations.tx;

import org.corfudb.generator.Correctness;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

/**
 * Write after write transaction operation
 */
public class WriteAfterWriteTxOperation extends AbstractTxOperation {

    public WriteAfterWriteTxOperation(State state, Operations operations, CorfuTablesGenerator tablesManager) {
        super(state, Operation.Type.TX_WAW, operations, tablesManager);
    }

    @Override
    public void execute() {
        Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_START);
        long timestamp;
        startWriteAfterWriteTx();

        executeOperations();
        try {
            timestamp = stopTx();
            Correctness.recordTransactionMarkers(true, opType.getOpType(), Correctness.TX_END,
                    Long.toString(timestamp));
        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, opType.getOpType(), Correctness.TX_ABORTED);
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
