package org.corfudb.infrastructure.log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;

/**
 * This class implements the StreamLog interface using a Java hash map. The stream log is only stored in-memory and not
 * persisted and thus should only be used for testing.
 *
 * Created by maithem on 7/21/16.
 */
public class InMemoryStreamLog implements StreamLog {

    private Map<Long, LogData> cache;

    public InMemoryStreamLog() {
        cache = new ConcurrentHashMap();
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        if(cache.containsKey(address)) {
            throw new OverwriteException();
        }
        cache.put(address, entry);
    }

    @Override
    public LogData read(long address) {
        return cache.get(address);
    }

    @Override
    public void sync(){
        //no-op
    }

    @Override
    public void close() {
        cache = new HashMap();
    }
}
