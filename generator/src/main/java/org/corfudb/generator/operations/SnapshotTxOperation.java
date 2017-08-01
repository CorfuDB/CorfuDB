package org.corfudb.generator.operations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.State;

import java.util.List;
import java.util.Random;

/**
 * Created by box on 7/15/17.
 */
@Slf4j
public class SnapshotTxOperation extends Operation {

    public SnapshotTxOperation(State state) {
        super(state);
    }

    @Override
    public void execute() {
        long trimMark = state.getTrimMark();
        Random rand = new Random();
        long delta = (long) rand.nextInt(10) + 1;
        state.startSnapshotTx(trimMark + delta);

        int numOperations = state.getOperationCount().sample(1).get(0);
        List<Operation> operations = state.getOperations().sample(numOperations);

        for (int x = 0; x < operations.size(); x++) {
            if (operations.get(x) instanceof OptimisticTxOperation
                    || operations.get(x) instanceof SnapshotTxOperation
                    || operations.get(x) instanceof RemoveOperation
                    || operations.get(x) instanceof WriteOperation) {
                continue;
            }

            operations.get(x).execute();
        }

        state.stopSnapshotTx();
    }
}
