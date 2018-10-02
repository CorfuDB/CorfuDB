package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.generator.replayer.Event;

import java.util.function.Predicate;

/**
 * Created by Sam Behnam on 2/28/18.
 */
public class ScanAndFilterOperation extends Operation {
    public ScanAndFilterOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        final SMRMap<String, Object> map = (SMRMap<String, Object>) getConfiguration()
                .getMap(event.getMapId());

        // Using an arbitrary predicate to simulate scanAndFilter
        final Predicate<Object> valuePredicate = p -> p.equals("valueObject");
        return map.scanAndFilter(valuePredicate);
    }
}
