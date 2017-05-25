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
    private Set<LogAddress> trimmed;
    final private AtomicLong globalTail = new AtomicLong(0L);
    private volatile long startingAddress;

    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap();
        streamCache = new HashMap();
        trimmed = new ConcurrentSet<>();
        startingAddress = -1;
    }

    @Override
    public synchronized void append(LogAddress logAddress, LogData entry) {
        try {
            checkPrefix(logAddress.getAddress());
        } catch (TrimmedException e) {
            throw new OverwriteException();
        }

        if (logAddress.getStream() == null) {
            if(logCache.containsKey(logAddress.address)) {
                throwLogUnitExceptionsIfNecessary(logAddress, entry);
            }
            logCache.put(logAddress.address, entry);
        } else {

            Map<Long, LogData> stream = streamCache.get(logAddress.getStream());
            if(stream == null) {
                stream = new HashMap();
                streamCache.put(logAddress.getStream(), stream);
            }

            if(stream.containsKey(logAddress.address)) {
                throwLogUnitExceptionsIfNecessary(logAddress, entry);
            }
            stream.put(logAddress.address, entry);
        }

        globalTail.getAndUpdate(maxTail -> entry.getGlobalAddress() > maxTail ? entry.getGlobalAddress() : maxTail);
    }

    private void checkPrefix(long address) {
        if(address <= startingAddress) {
            throw new TrimmedException();
        }
    }

    @Override
    public synchronized void prefixTrim(LogAddress logAddress) {
        checkPrefix(logAddress.getAddress());
        startingAddress =logAddress.getAddress();
    }

    @Override
    public long getGlobalTail() {
        return globalTail.get();
    }

    private void throwLogUnitExceptionsIfNecessary(LogAddress logAddress, LogData entry) {
        if (entry.getRank()==null) {
            throw new OverwriteException();
        } else {
            // the method below might throw DataOutrankedException or ValueAdoptedException
            assertAppendPermittedUnsafe(logAddress, entry);
        }
    }

    @Override
    public synchronized void trim(LogAddress logAddress) {
        trimmed.add(logAddress);
    }

    @Override
    public LogData read(LogAddress logAddress) {
        checkPrefix(logAddress.getAddress());

        if(trimmed.contains(logAddress)) {
            throw new TrimmedException();
        }

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
    public void sync(boolean force){
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

    @Override
    public synchronized void compact() {
        // Prefix Trim
        for(long address : logCache.keySet()){
            if (address < startingAddress) {
                logCache.remove(address);
            }
        }

        // Sparse trim
        for (LogAddress logAddress : trimmed) {

            if(logAddress.getStream() == null) {
                logCache.remove(logAddress.address);
            } else {
                Map<Long, LogData> stream = streamCache.get(logAddress.getStream());
                if(stream != null){
                    stream.remove(logAddress.address);
                }
            }
        }

        for (LogAddress logAddress : trimmed) {
            if (logAddress.getAddress() < startingAddress) {
                trimmed.remove(logAddress);
            }
        }
    }
}
