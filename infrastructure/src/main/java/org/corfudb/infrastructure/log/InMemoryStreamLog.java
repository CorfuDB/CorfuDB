package org.corfudb.infrastructure.log;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.internal.ConcurrentSet;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;

/**
 * This class implements the StreamLog interface using a Java hash map. The stream log is only stored in-memory and not
 * persisted and thus should only be used for testing.
 *
 * Created by maithem on 7/21/16.
 */
public class InMemoryStreamLog implements StreamLog, StreamLogWithRankedAddressSpace {

    private Map<Long, LogData> logCache;
    private Map<UUID, Map<Long, LogData>> streamCache;
    private Set<Long> trimmed;
    final private AtomicLong globalTail = new AtomicLong(0L);
    private volatile long startingAddress;

    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap();
        streamCache = new HashMap();
        trimmed = new ConcurrentSet<>();
        startingAddress = -1;
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        try {
            checkRange(address);
        } catch (TrimmedException e) {
            throw new OverwriteException();
        }

        if (logCache.containsKey(address)) {
            throwLogUnitExceptionsIfNecessary(address, entry);
        }
        logCache.put(address, entry);


        globalTail.getAndUpdate(maxTail -> entry.getGlobalAddress() > maxTail ? entry.getGlobalAddress() : maxTail);
    }

    private void checkRange(long address) {
        if(address < startingAddress) {
            throw new TrimmedException();
        }
    }

    @Override
    public synchronized void prefixTrim(long address) {
        checkRange(address);
        startingAddress = address + 1;
    }

    @Override
    public long getGlobalTail() {
        return globalTail.get();
    }

    private void throwLogUnitExceptionsIfNecessary(long address, LogData entry) {
        if (entry.getRank()==null) {
            throw new OverwriteException();
        } else {
            // the method below might throw DataOutrankedException or ValueAdoptedException
            assertAppendPermittedUnsafe(address, entry);
        }
    }

    @Override
    public synchronized void trim(long address) {
        trimmed.add(address);
    }

    @Override
    public LogData read(long address) {
        checkRange(address);
        if(trimmed.contains(address)) {
            throw new TrimmedException();
        }

        return logCache.get(address);
    }

    @Override
    public void sync(boolean force){
        //no-op
    }

    @Override
    public void close() {
        logCache = new HashMap();
        streamCache = new HashMap();
    }

    @Override
    public void release(long address, LogData entry) {
        // in memory, do nothing
    }

    @Override
    public synchronized void compact() {
        // Prefix Trim
        for(long address : logCache.keySet()){
            if (address < startingAddress) {
                logCache.remove(address);
            }
        }

        // Sparse trim
        for (long address : trimmed) {
            logCache.remove(address);
        }

        for (long address : trimmed) {
            if (address < startingAddress) {
                trimmed.remove(address);
            }
        }
    }
}
