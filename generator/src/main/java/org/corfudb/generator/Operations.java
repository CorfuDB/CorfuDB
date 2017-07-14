package org.corfudb.generator;

import com.google.common.collect.ImmutableList;

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
