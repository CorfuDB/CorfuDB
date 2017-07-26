package org.corfudb.generator.operations;

import org.corfudb.generator.State;

/**
 *
 * A definition of a generic operation that the generator can execute.
 *
 * Created by maithem on 7/14/17.
 */
public abstract class Operation {
    protected State state;
    String shortName;

    public Operation(State state) {
        this.state = state;
    }

    public abstract void execute();
}
