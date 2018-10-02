package org.corfudb.generator.replayer.replayOperations;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.generator.replayer.Event;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Created by Sam Behnam on 2/28/18.
 */
public class ScanAndFilterByEntryOperation extends Operation {
    public ScanAndFilterByEntryOperation(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Object execute(Event event) {
        final SMRMap<String, Object> map = (SMRMap<String, Object>) getConfiguration()
                .getMap(event.getMapId());

        // Using an arbitrary predicate to simulate scanAndFilterByEntry
        Predicate<Map.Entry<String, Object>> valuePredicate =
                p -> p.getValue().equals("valueObject");
        return map.scanAndFilterByEntry(valuePredicate);
    }
}
