package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

import java.util.Map;

/**
 * Created by Sam Behnam on 2/3/18.
 */
public class PutOperation extends Operation {
    public PutOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        final Map<String, Object> map = getConfiguration()
                .getMap(event.getMapId());
        final String key = event.getKey();
        final Object value = OperationUtils
                .getValueObjectForSize(event.getValueSize());

        return map.put(key, value);
    }
}
