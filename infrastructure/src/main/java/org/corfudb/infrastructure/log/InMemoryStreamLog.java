package org.corfudb.infrastructure.log;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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

    private Map<Long, LogData> logCache;
    private Map<UUID, Map<Long, LogData>> streamCache;

    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap();
        streamCache = new HashMap();
    }

    @Override
    public synchronized void append(LogAddress logAddress, LogData entry) {
        if (logAddress.getStream() == null) {
            if(logCache.containsKey(logAddress.address)) {
                throw new OverwriteException();
            }
            logCache.put(logAddress.address, entry);
        } else {

            Map<Long, LogData> stream = streamCache.get(logAddress.getStream());
            if(stream == null) {
                stream = new HashMap();
                streamCache.put(logAddress.getStream(), stream);
            }

            if(stream.containsKey(logAddress.address)) {
                throw new OverwriteException();
            } else {
                stream.put(logAddress.address, entry);
            }
        }
    }

    @Override
    public LogData read(LogAddress logAddress) {
        if (logAddress.getStream() == null) {
            return logCache.get(logAddress.address);
        } else {

            Map<Long, LogData> stream = streamCache.get(logAddress.getStream());
            if (stream == null) {
                return null;
            } else {
                return stream.get(logAddress.address);
            }
        }
    }

    @Override
    public void sync(){
        //no-op
    }

    @Override
    public void close() {
        logCache = new HashMap();
        streamCache = new HashMap();
    }

    @Override
    public void release(LogAddress logAddress, LogData entry) {
        // in memory, do nothing
    }
}
