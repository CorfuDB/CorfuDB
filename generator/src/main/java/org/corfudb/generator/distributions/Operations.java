package org.corfudb.generator.distributions;

import com.google.common.collect.ImmutableList;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.operations.RemoveOperation;
import org.corfudb.generator.operations.SleepOperation;
import org.corfudb.generator.operations.WriteOperation;
import org.corfudb.generator.operations.tx.NestedTxOperation;
import org.corfudb.generator.operations.tx.OptimisticTxOperation;
import org.corfudb.generator.operations.tx.SnapshotTxOperation;
import org.corfudb.generator.operations.tx.WriteAfterWriteTxOperation;
import org.corfudb.generator.state.State;

import java.util.List;

/**
 * This class implements a distribution of all possible operations that the generator
 * can execute.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Operations implements DataSet<Operation.Type> {

    private final List<Operation.Type> allOperations;
    private final State state;
    private final State.CorfuTablesGenerator tablesManager;

    public Operations(State state, State.CorfuTablesGenerator tablesManager) {
        this.state = state;
        this.tablesManager = tablesManager;

        allOperations = ImmutableList.of(
                Operation.Type.WRITE, Operation.Type.READ, Operation.Type.TX_OPTIMISTIC,
                Operation.Type.TX_SNAPSHOT, Operation.Type.SLEEP, Operation.Type.REMOVE
        );
    }

    public void populate() {
        //no-op
    }

    public Operation getRandomOperation() {
        Operation.Type opType = allOperations.get(RANDOM.nextInt(allOperations.size()));
        switch (opType) {
            case READ:
                return new ReadOperation(state, tablesManager);
            case REMOVE:
                return new RemoveOperation(state, tablesManager);
            case SLEEP:
                return new SleepOperation(state);
            case WRITE:
                return new WriteOperation(state);
            case TX_NESTED:
                return new NestedTxOperation(state);
            case TX_OPTIMISTIC:
                return new OptimisticTxOperation(state);
            case TX_SNAPSHOT:
                return new SnapshotTxOperation(state);
            case TX_WAW:
                return new WriteAfterWriteTxOperation(state);
            case CHECKPOINT:
                return new CheckpointOperation(state);
            default:
                throw new IllegalStateException("Unexpected type of operation: " + opType);
        }
    }

    public List<Operation.Type> getDataSet() {
        return allOperations;
    }
}
