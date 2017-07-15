package org.corfudb.generator.distributions;

import com.google.common.collect.ImmutableList;
import org.corfudb.generator.State;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.OptimisticTxOperation;
import org.corfudb.generator.operations.ReadOperation;
import org.corfudb.generator.operations.SleepOperation;
import org.corfudb.generator.operations.WriteOperation;

import java.util.List;

/**
 * Created by maithem on 7/14/17.
 */
public class Operations extends DataSet {

    final private List<Operation> operations;

    public Operations(State state) {
        operations = ImmutableList.of(
                new WriteOperation(state),
                new ReadOperation(state),
                new OptimisticTxOperation(state),
                new SleepOperation(state));
    }

    public void populate() {
        //no-op
    }

    public List<Operation> getDataSet() {
        return operations;
    }
}
