package org.corfudb.generator.operations;

import org.corfudb.generator.State;

/**
 * Created by maithem on 7/14/17.
 */
public abstract class Operation {
    protected State state;
    public Operation(State state) {
        this.state = state;
    }

    public abstract void execute();
}
