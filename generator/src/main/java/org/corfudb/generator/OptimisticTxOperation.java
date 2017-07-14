package org.corfudb.generator;

import java.util.List;

/**
 * Created by maithem on 7/14/17.
 */
public class OptimisticTxOperation extends Operation {

    public OptimisticTxOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {

        state.startOptimisticTx();

        int numOperations = state.getOperationCount().sample(1).get(0);
        List<Operation> operations = state.getOperations().sample(numOperations);

        for (int x = 0; x < operations.size(); x++) {
            operations.get(x).execute();
        }

        state.stopOptimisticTx();
    }
}
