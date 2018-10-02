package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.generator.replayer.Event;

import java.util.Map;

/**
 * Created by Sam Behnam on 2/2/18.
 */
public class GetOperation extends Operation {

    public GetOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        final Map<String, Object> map = getConfiguration().getMap(event.getMapId());
        final String key = event.getKey();
        return map.get(key);
    }
}
