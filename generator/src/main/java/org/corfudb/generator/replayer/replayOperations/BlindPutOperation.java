package org.corfudb.generator.replayer.replayOperations;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.generator.replayer.Event;


/**
 * Created by Sam Behnam on 2/14/18.
 */
@Slf4j
public class BlindPutOperation extends Operation {
    public BlindPutOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        // Casting as blindPut is only implemented on the ISMRMap.
        // Initialization must have added appropriate ISMRMap to configuration
        final ISMRMap<String, Object> map = (ISMRMap<String, Object>) getConfiguration()
                .getMap(event.getMapId());
        final String key = event.getKey();
        final Object value = OperationUtils.getValueObjectForSize(event.getValueSize());

        map.blindPut(key, value);
        return null;
    }
}
