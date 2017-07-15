package org.corfudb.generator.operations;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;

/**
 * Created by maithem on 7/14/17.
 */
@Slf4j
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
            if (operations.get(x) instanceof OptimisticTxOperation) {
                continue;
            }

            operations.get(x).execute();
        }

        state.stopOptimisticTx();
    }
}
