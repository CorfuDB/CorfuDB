package org.corfudb.generator.operations.tx;

import org.corfudb.generator.Correctness;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

/**
 * Write after write transaction operation
 */
public class WriteAfterWriteTxOperation extends AbstractTxOperation {

    public WriteAfterWriteTxOperation(State state) {
        super(state, Operation.Type.TX_WAW);
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

    public void startWriteAfterWriteTx() {
        state.getRuntime().getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }
}
