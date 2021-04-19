package org.corfudb.generator.distributions;

import com.google.common.collect.ImmutableList;
import org.corfudb.generator.correctness.Correctness;
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
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;

import java.util.List;

/**
 * This class implements a distribution of all possible operations that the generator
 * can execute.
 * <p>
 * Created by maithem on 7/14/17.
 */
public class Operations implements DataSet<Operation.Type> {

    private final List<Operation.Type> allOperationTypes;
    private final State state;
    private final CorfuTablesGenerator tablesManager;
    private final Correctness correctness;

    public Operations(State state, CorfuTablesGenerator tablesManager, Correctness correctness) {
        this.state = state;
        this.tablesManager = tablesManager;
        this.correctness = correctness;

        allOperationTypes = ImmutableList.of(
                Operation.Type.WRITE, Operation.Type.READ, Operation.Type.TX_OPTIMISTIC,
                Operation.Type.TX_SNAPSHOT, Operation.Type.SLEEP, Operation.Type.REMOVE
        );
    }

    public void populate() {
        //no-op
    }

    public Operation getRandomOperation() {
        Operation.Type opType = allOperationTypes.get(RANDOM.nextInt(allOperationTypes.size()));
        return create(opType);
    }

    public List<Operation.Type> getDataSet() {
        return allOperationTypes;
    }

    public Operation create(Operation.Type opType) {
        switch (opType) {
            case READ:
                return new ReadOperation(state, tablesManager, correctness);
            case REMOVE:
                return new RemoveOperation(state, tablesManager, correctness);
            case SLEEP:
                return new SleepOperation(state);
            case WRITE:
                return new WriteOperation(state, tablesManager, correctness);
            case TX_NESTED:
                return new NestedTxOperation(state, this, tablesManager, correctness);
            case TX_OPTIMISTIC:
                return new OptimisticTxOperation(state, this, tablesManager, correctness);
            case TX_SNAPSHOT:
                return new SnapshotTxOperation(state, this, tablesManager, correctness);
            case TX_WAW:
                return new WriteAfterWriteTxOperation(state, this, tablesManager, correctness);
            case CHECKPOINT:
                return new CheckpointOperation(state, tablesManager);
            default:
                throw new IllegalStateException("Unexpected type of operation: " + opType);
        }
    }
}
