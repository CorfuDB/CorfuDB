package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/15/18.
 */
public class RemoveOperation extends Operation {
    public RemoveOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        return getConfiguration()
                .getMap(event.getMapId())
                .remove(event.getKey());
    }
}
