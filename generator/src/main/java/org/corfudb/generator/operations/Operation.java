package org.corfudb.generator.operations;

import org.corfudb.generator.state.State;

/**
 * A definition of a generic operation that the generator can execute.
 */
public abstract class Operation {
    protected final State state;
    protected final String shortName;

    public Operation(State state, String shortName) {
        this.state = state;
        this.shortName = shortName;
    }

    public abstract void execute();

    public abstract boolean verify();

    public abstract void addToHistory();
}
