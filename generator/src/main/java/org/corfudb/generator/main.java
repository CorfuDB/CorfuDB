package org.corfudb.generator;

import java.util.List;

import org.corfudb.runtime.CorfuRuntime;

/**
 * Created by maithem on 7/14/17.
 */
public class main {
    public static void main(String[] args) {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        State state = new State(100, 10000, rt);

        List<Operation> operations = state.getOperations().sample(100);

        for (Operation operation : operations) {
            operation.execute();
        }
    }
}
