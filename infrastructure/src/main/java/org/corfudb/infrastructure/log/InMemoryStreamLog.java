package org.corfudb.infrastructure.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;

import lombok.extern.slf4j.Slf4j;

/**
 * This class implements the StreamLog interface using a Java hash map.
 * The stream log is only stored in-memory and not persisted.
 * This should only be used for testing.
 * Created by maithem on 7/21/16.
 */
@Slf4j
public class InMemoryStreamLog implements StreamLog, StreamLogWithRankedAddressSpace {

    private final AtomicLong globalTail = new AtomicLong(0L);
    private Map<Long, LogData> logCache;
    private Set<Long> trimmed;
    private volatile long startingAddress;

    /**
     * Returns an object that stores a stream log in memory.
     */
    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap();
        trimmed = ConcurrentHashMap.newKeySet();
        startingAddress = 0;
    }

    @Override
    public synchronized void append(List<LogData> entries) {
        for (LogData entry : entries) {
            if (isTrimmed(entry.getGlobalAddress()) || logCache.containsKey(entry.getGlobalAddress())) {
                continue;
            }

            logCache.put(entry.getGlobalAddress(), entry);
            globalTail.getAndUpdate(maxTail -> entry.getGlobalAddress() > maxTail
                    ? entry.getGlobalAddress() : maxTail);
        }
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        if(isTrimmed(address)) {
            throw new OverwriteException();
        }

        if (logCache.containsKey(address)) {
            throwLogUnitExceptionsIfNecessary(address, entry);
        }
        logCache.put(address, entry);


        globalTail.getAndUpdate(maxTail -> entry.getGlobalAddress() > maxTail
                ? entry.getGlobalAddress() : maxTail);
    }

    private boolean isTrimmed(long address) {
        if (address < startingAddress) {
            return true;
        }
        return false;
    }

    @Override
    public synchronized void prefixTrim(long address) {
        if (isTrimmed(address)) {
            log.warn("prefixTrim: Ignoring repeated trim {}", address);
        } else {
            startingAddress = address + 1;
        }
    }

    @Override
    public long getGlobalTail() {
        return globalTail.get();
    }

    @Override
    public long getTrimMark() {
        return startingAddress;
    }

    private void throwLogUnitExceptionsIfNecessary(long address, LogData entry) {
        if (entry.getRank() == null) {
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
        if (isTrimmed(address)) {
            return LogData.getTrimmed(address);
        }
        if (trimmed.contains(address)) {
            return LogData.getTrimmed(address);
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
    }

    @Override
    public void release(long address, LogData entry) {
        // in memory, do nothing
    }

    @Override
    public synchronized void compact() {
        // Prefix Trim
        for (long address : logCache.keySet()) {
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

    @Override
    public void reset() {
        startingAddress = 0;
        globalTail.set(0L);
        // Clear the trimmed addresses record.
        trimmed.clear();
        // Clearing all data from the cache.
        logCache.clear();
    }
}
