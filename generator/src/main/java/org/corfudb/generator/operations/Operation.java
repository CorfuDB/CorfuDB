package org.corfudb.generator.operations;

import org.corfudb.generator.State;

/**
 * A definition of a generic operation that the generator can execute.
 * <p>
 * Created by maithem on 7/14/17.
 */
public abstract class Operation {
    protected final State state;
    protected final String shortName;

    public Operation(State state, String shortName) {
        this.state = state;
        this.shortName = shortName;
    }

    public abstract void execute();
}
