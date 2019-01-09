package org.corfudb.infrastructure.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;

/**
 * This class implements the StreamLog interface using a Java hash map.
 * The stream log is only stored in-memory and not persisted.
 * This should only be used for testing.
 * Created by maithem on 7/21/16.
 */
@Slf4j
public class InMemoryStreamLog implements StreamLog, StreamLogWithRankedAddressSpace {

    private Map<Long, LogData> logCache;
    private Set<Long> trimmed;
    private volatile long startingAddress;
    private volatile LogMetadata logMetadata;

    /**
     * Returns an object that stores a stream log in memory.
     */
    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap();
        trimmed = ConcurrentHashMap.newKeySet();
        startingAddress = 0;
        logMetadata = new LogMetadata();
    }

    @Override
    public synchronized void append(List<LogData> entries) {
        for (LogData entry : entries) {
            if (isTrimmed(entry.getGlobalAddress()) || logCache.containsKey(entry.getGlobalAddress())) {
                continue;
            }

            logCache.put(entry.getGlobalAddress(), entry);
            logMetadata.update(entry);
        }
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        if(isTrimmed(address)) {
            throw new OverwriteException(OverwriteCause.TRIM);
        }

        if (logCache.containsKey(address)) {
            throwLogUnitExceptionsIfNecessary(address, entry);
        }
        logCache.put(address, entry);
        logMetadata.update(entry);
    }

    private boolean isTrimmed(long address) {
        return address < startingAddress || trimmed.contains(address);
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
    public synchronized TailsResponse getTails() {
        Map<UUID, Long> tails = new HashMap<>(logMetadata.getStreamTails().size());
        for (Map.Entry<UUID, Long> entry : logMetadata.getStreamTails().entrySet()) {
            tails.put(entry.getKey(), entry.getValue());
        }
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public long getTrimMark() {
        return startingAddress;
    }

    private void throwLogUnitExceptionsIfNecessary(long address, LogData entry) {
        if (entry.getRank() == null) {
            OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
            log.trace("throwLogUnitExceptionsIfNecessary: overwritten exception for address {}, cause: {}", address, overwriteCause);
            throw new OverwriteException(overwriteCause);
        } else {
            // the method below might throw DataOutrankedException or ValueAdoptedException
            assertAppendPermittedUnsafe(address, entry);
        }
    }

    @Override
    public LogData read(long address) {
        if (isTrimmed(address)) {
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
        logMetadata = new LogMetadata();
        // Clear the trimmed addresses record.
        trimmed.clear();
        // Clearing all data from the cache.
        logCache.clear();
    }
}
