package org.corfudb.generator.operations.tx;

import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;

import java.util.List;

/**
 * Abstract class for all transactional operations
 */
public abstract class AbstractTxOperation extends Operation {

    public AbstractTxOperation(State state, String shortName) {
        super(state, shortName);
    }

    protected void executeOperations() {
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
    }
}
