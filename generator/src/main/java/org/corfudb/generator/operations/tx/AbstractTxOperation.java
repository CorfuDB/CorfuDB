package org.corfudb.generator.operations.tx;

import com.google.common.collect.ImmutableList;
import org.corfudb.generator.correctness.Correctness;
import org.corfudb.generator.distributions.Operations;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.state.CorfuTablesGenerator;
import org.corfudb.generator.state.State;

import java.util.List;

/**
 * Abstract class for all transactional operations
 */
public abstract class AbstractTxOperation extends Operation {

    protected final Operations operations;
    protected final CorfuTablesGenerator tablesManager;
    protected final Correctness correctness;

    public AbstractTxOperation(State state, Operation.Type operationType, Operations operations,
                               CorfuTablesGenerator tablesManager, Correctness correctness) {
        super(state, operationType);
        this.operations = operations;
        this.tablesManager = tablesManager;
        this.correctness = correctness;
    }

    protected List<Operation> createOperations() {
        int numOperations = state.getOperationCount().sample();
        List<Operation.Type> operationTypes = operations.sample(numOperations);

        return operationTypes.stream()
                .filter(Type::notTxOptimisticOrNestedOrSnapshot)
                .map(operations::create)
                .collect(ImmutableList.toImmutableList());
    }

    protected void executeOperations() {
        for (Operation nestedOperation : getNestedOperations()) {
            nestedOperation.execute();
        }
    }

    public abstract List<Operation> getNestedOperations();

    protected long stopTx() {
        return tablesManager.getRuntime().getObjectsView().TXEnd();
    }

    protected void startOptimisticTx() {
        tablesManager.getRuntime().getObjectsView().TXBegin();
    }
}
