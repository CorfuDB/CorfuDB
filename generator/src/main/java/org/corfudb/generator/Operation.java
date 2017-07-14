package org.corfudb.generator;

/**
 * Created by maithem on 7/14/17.
 */
public abstract class Operation {
    protected State state;
    public Operation(State state) {
        this.state = state;
    }

    abstract void execute();
}
