package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/5/18.
 */
public class NoOperation extends Operation {
    public NoOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        // No-Op
        return null;
    }
}
