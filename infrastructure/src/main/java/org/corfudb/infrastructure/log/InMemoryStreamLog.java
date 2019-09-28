package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.log.StreamLog.assertAppendPermittedUnsafe;
import static org.corfudb.infrastructure.log.StreamLog.getOverwriteCauseForAddress;

/**
 * This class implements the StreamLog interface using a Java hash map.
 * The stream log is only stored in-memory and not persisted.
 * This should only be used for testing.
 * Created by maithem on 7/21/16.
 */
@Slf4j
public class InMemoryStreamLog implements StreamLog {

    private Map<Long, LogData> logCache;
    private volatile LogMetadata logMetadata;
    private AtomicLong committedTail;

    /**
     * Returns an object that stores a stream log in memory.
     */
    public InMemoryStreamLog() {
        logCache = new ConcurrentHashMap<>();
        logMetadata = new LogMetadata();
        committedTail = new AtomicLong(Address.NON_ADDRESS);
    }

    @Override
    public synchronized void append(List<LogData> entries) {
        for (LogData entry : entries) {
            if (logCache.containsKey(entry.getGlobalAddress())) {
                continue;
            }

            logCache.put(entry.getGlobalAddress(), entry);
        }
        logMetadata.update(entries);
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        if (logCache.containsKey(address)) {
            throwLogUnitExceptionsIfNecessary(address, entry);
        }
        logCache.put(address, entry);
        logMetadata.update(Collections.singletonList(entry));
    }

    @Override
    public synchronized TailsResponse getTails(List<UUID> streams) {
        Map<UUID, Long> tails = new HashMap<>(streams.size());
        for(UUID stream: streams) {
            tails.put(stream, logMetadata.getStreamTails().get(stream));
        }
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public synchronized long getLogTail() {
        return logMetadata.getGlobalTail();
    }

    @Override
    public synchronized TailsResponse getAllTails() {
        Map<UUID, Long> tails = new HashMap<>(logMetadata.getStreamTails().size());
        for (Map.Entry<UUID, Long> entry : logMetadata.getStreamTails().entrySet()) {
            tails.put(entry.getKey(), entry.getValue());
        }
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public long getCommittedTail() {
        return committedTail.get();
    }

    @Override
    public synchronized void updateCommittedTail(long newCommittedTail) {
        committedTail.updateAndGet(curr -> {
            if (newCommittedTail <= curr) {
                return curr;
            }
            return newCommittedTail;
        });
    }

    @Override
    public synchronized StreamsAddressResponse getStreamsAddressSpace() {
        return new StreamsAddressResponse(logMetadata.getGlobalTail(), logMetadata.getStreamsAddressSpaceMap());
    }

    private void throwLogUnitExceptionsIfNecessary(long address, LogData entry) {
        LogData currentEntry = read(address);
        if (entry.getRank() == null) {
            OverwriteCause overwriteCause = getOverwriteCauseForAddress(currentEntry, entry);
            log.trace("throwLogUnitExceptionsIfNecessary: overwritten exception for address {}, cause: {}", address, overwriteCause);
            throw new OverwriteException(overwriteCause);
        } else {
            // the method below might throw DataOutrankedException or ValueAdoptedException
            assertAppendPermittedUnsafe(address, currentEntry, entry);
        }
    }

    /**
     * Returns the known addresses in this Log Unit in the specified consecutive
     * range of addresses.
     *
     * @param rangeStart Start address of range.
     * @param rangeEnd   End address of range.
     * @return Set of known addresses.
     */
    @Override
    public Set<Long> getKnownAddressesInRange(long rangeStart, long rangeEnd) {

        Set<Long> result = new HashSet<>();
        for (long address = rangeStart; address <= rangeEnd; address++) {
            if (logCache.containsKey(address)) {
                result.add(address);
            }
        }
        return result;
    }

    @Override
    public LogData read(long address) {
        return logCache.get(address);
    }

    @Override
    public boolean contains(long address) {
        return logCache.containsKey(address);
    }

    @Override
    public LogData readGarbageEntry(long address) {
        return null;
    }

    @Override
    public long getGlobalCompactionMark() {
        return Address.NON_ADDRESS;
    }

    @Override
    public void sync(boolean force){
        // No-op
    }

    @Override
    public void close() {
        logCache = new HashMap<>();
    }

    @Override
    public void startCompactor() {
        // No-op
    }

    @Override
    public void reset() {
        logMetadata = new LogMetadata();
        // Clearing all data from the cache.
        logCache.clear();
    }
}
