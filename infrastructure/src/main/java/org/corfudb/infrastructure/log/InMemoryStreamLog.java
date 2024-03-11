package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig.UPDATE_INTERVAL_SECONDS;

/**
 * This class implements the StreamLog interface using a Java hash map.
 * The stream log is only stored in-memory and not persisted.
 * This should only be used for testing.
 * Created by maithem on 7/21/16.
 */
@Slf4j
public class InMemoryStreamLog implements StreamLog {

    private Map<Long, LogData> logCache;
    private final Set<Long> trimmed;
    private volatile long startingAddress;
    private volatile LogMetadata logMetadata;
    private AtomicLong committedTail;
    private final FileSystemAgent fsAgent;
    private final StreamLogDataStore dataStore;

    /**
     * Returns an object that stores a stream log in memory.
     */
    public InMemoryStreamLog(BatchProcessorContext batchProcessorContext) {
        logCache = new ConcurrentHashMap<>();
        trimmed = ConcurrentHashMap.newKeySet();
        startingAddress = 0;
        Map<String, Object> opts = new HashMap<>();
        opts.put("--memory", true);
        this.dataStore = new StreamLogDataStore(new DataStore(opts, fileName -> {
        }));
        logMetadata = new LogMetadata(dataStore);
        committedTail = new AtomicLong(Address.NON_ADDRESS);

        Path dummyLogDir = new File(".").toPath().toAbsolutePath();
        double unlimited = 100;
        long reservedSpace = 0;
        FileSystemConfig config = new FileSystemConfig(dummyLogDir, unlimited, reservedSpace,
                PersistenceMode.MEMORY, UPDATE_INTERVAL_SECONDS);
        fsAgent = FileSystemAgent.init(config, batchProcessorContext);
    }

    @Override
    public synchronized void append(List<LogData> entries) {
        for (LogData entry : entries) {
            if (isTrimmed(entry.getGlobalAddress()) || logCache.containsKey(entry.getGlobalAddress())) {
                continue;
            }

            logCache.put(entry.getGlobalAddress(), entry);
            logMetadata.update(entry, false);
        }
    }

    @Override
    public synchronized void append(long address, LogData entry) {
        if(isTrimmed(address)) {
            throw new OverwriteException(OverwriteCause.TRIM);
        }

        if (logCache.containsKey(address)) {
            OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
            log.trace("throwLogUnitExceptionsIfNecessary: overwritten exception for address {}, cause: {}", address, overwriteCause);
            throw new OverwriteException(overwriteCause);
        }
        logCache.put(address, entry);
        logMetadata.update(entry, false);
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

            // Trim address space maps.
            logMetadata.prefixTrim(address);
        }
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
    public synchronized StreamsAddressResponse getStreamsAddressSpace() {
        return new StreamsAddressResponse(logMetadata.getGlobalTail(), logMetadata.getStreamsAddressSpaceMap());
    }

    @Override
    public long getCommittedTail() {
        return committedTail.get();
    }

    @Override
    public void updateCommittedTail(long newCommittedTail) {
        committedTail.updateAndGet(curr -> {
            if (newCommittedTail <= curr) {
                return curr;
            }
            return newCommittedTail;
        });
    }

    @Override
    public long getTrimMark() {
        return startingAddress;
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
        if (isTrimmed(address)) {
            return LogData.getTrimmed(address);
        }

        return logCache.get(address);
    }

    @Override
    public boolean contains(long address) throws TrimmedException {
        if (isTrimmed((address))) {
            throw new TrimmedException();
        }

        if (address <= getCommittedTail()) {
            return true;
        }

        return logCache.containsKey(address);
    }

    @Override
    public void sync(boolean force){
        //no-op
    }

    @Override
    public void close() {
        logCache = new HashMap<>();
        fsAgent.shutdown();
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
        logMetadata = new LogMetadata(dataStore);
        // Clear the trimmed addresses record.
        trimmed.clear();
        // Clearing all data from the cache.
        logCache.clear();
    }
}
