package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/15/18.
 */
public class ClearOperation extends Operation {
    public ClearOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        getConfiguration()
                .getMap(event.getMapId())
                .clear();
        return null;
    }
}
