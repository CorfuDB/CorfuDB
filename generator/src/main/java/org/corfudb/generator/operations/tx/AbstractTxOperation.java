package org.corfudb.generator.operations.tx;

import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.State;

import java.util.List;

/**
 * Abstract class for all transactional operations
 */
public abstract class AbstractTxOperation extends Operation {

    public AbstractTxOperation(State state, Operation.Type operationType) {
        super(state, operationType);
    }

    protected void executeOperations() {
        int numOperations = state.getOperationCount().sample();
        List<Operation> operations = state.getOperations().sample(numOperations);

        for (Operation operation : operations) {
            Type opType = operation.getOpType();
            if (opType == Type.TX_OPTIMISTIC || opType == Type.TX_SNAPSHOT || opType == Type.TX_NESTED) {
                continue;
            }

            operation.execute();
        }
    }

    protected long stopTx() {
        return state.getRuntime().getObjectsView().TXEnd();
    }

    protected void startOptimisticTx() {
        state.getRuntime().getObjectsView().TXBegin();
    }
}
