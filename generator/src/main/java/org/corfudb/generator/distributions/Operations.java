package org.corfudb.generator.distributions;

import com.google.common.collect.ImmutableList;
import org.corfudb.generator.State;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.OptimisticTxOperation;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.operations.RemoveOperation;
import org.corfudb.generator.operations.SleepOperation;
import org.corfudb.generator.operations.SnapshotTxOperation;
import org.corfudb.generator.operations.WriteOperation;

import java.util.List;

/**
 * This class implements a distribution of all possible operations that the generator
 * can execute.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Operations implements DataSet<Operation> {

    private final State state;
    private final List<Operation> allOperations;

    public Operations(State state) {
        allOperations = ImmutableList.of(
                new WriteOperation(state),
                new ReadOperation(state),
                new OptimisticTxOperation(state),
                new SnapshotTxOperation(state),
                new SleepOperation(state),
                new RemoveOperation(state)
        );
        this.state = state;
    }

    public void populate() {
        //no-op
    }

    public Operation getRandomOperation() {
        return allOperations.get(state.rand.nextInt(allOperations.size()));
    }

    public List<Operation> getDataSet() {
        return allOperations;
    }
}
