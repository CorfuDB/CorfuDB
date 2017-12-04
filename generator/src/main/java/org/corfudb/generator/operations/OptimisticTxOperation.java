package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.Correctness;
import org.corfudb.generator.State;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

import java.util.List;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
public class OptimisticTxOperation extends Operation {

    public OptimisticTxOperation(State state) {
        super(state);
        shortName = "TxOpt";
    }

    @Override
    public void execute() {
        try {
            Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_START);
            long timestamp;
            state.startOptimisticTx();

            int numOperations = state.getOperationCount().sample(1).get(0);
            List<Operation> operations = state.getOperations().sample(numOperations);

            for (int x = 0; x < operations.size(); x++) {
                if (operations.get(x) instanceof OptimisticTxOperation
                        || operations.get(x) instanceof SnapshotTxOperation
                        || operations.get(x) instanceof NestedTxOperation)
                {
                    continue;
                }

                operations.get(x).execute();
            }

            timestamp = state.stopTx();

            Correctness.recordTransactionMarkers(true, shortName, Correctness.TX_END,
                    Long.toString(timestamp));
            
            if (Address.isAddress(timestamp)) {
                state.setLastSuccessfulWriteOperationTimestamp(System.currentTimeMillis());
            }

        } catch (TransactionAbortedException tae) {
            Correctness.recordTransactionMarkers(false, shortName, Correctness.TX_ABORTED);
        }



    }
}
