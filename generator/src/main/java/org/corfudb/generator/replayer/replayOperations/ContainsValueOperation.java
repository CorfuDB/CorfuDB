package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

/**
 * Created by Sam Behnam on 2/15/18.
 */
public class ContainsValueOperation extends Operation {
    public ContainsValueOperation(Configuration configuration) {
        super(configuration);
    }

    // Contains values execute using an arbitrary value
    // with similar size of the value recorded for the event
    @Override
    public Object execute(Event event) {

        final Object arbitraryValueObjectForSize = OperationUtils.getValueObjectForSize(event.getValueSize());
        return getConfiguration()
                .getMap(event.getMapId())
                .containsValue(arbitraryValueObjectForSize);
    }
}
