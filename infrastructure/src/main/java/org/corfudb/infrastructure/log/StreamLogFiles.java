package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog {

    public static final int RECORDS_PER_LOG_FILE = 10000;
    private final Path logDir;

    private final StreamLogDataStore dataStore;

    private ConcurrentMap<Long, Segment> openSegments;
    private final Optional<AtomicLong> currentTrimMark;
    //=================Log Metadata=================
    // TODO(Maithem) this should effectively be final, but it is used
    // by a reset API that clears the state of this class, on reset
    // a new instance of this class should be created after deleting
    // the files of the old instance
    private LogMetadata logMetadata;

    // Resource quota to track the log size
    private final ResourceQuota logSizeQuota;

    private final String logUnitSizeMetricName = "logunit.size";
    private final String logUnitTrimMarkMetricName = "logunit.trimmark";
    /**
     * Prevents corfu from reading and executing maintenance
     * operations (reset log unit and stream log compaction) in parallel
     */
    private final ReadWriteLock resetLock = new ReentrantReadWriteLock();

    private final FileSystemAgent fsAgent;

    private final ServerContext sc;

    /**
     * Returns a file-based stream log object.
     *
     * @param serverContext Context object that provides server state such as epoch,
     *                      segment and start address
     */
    public StreamLogFiles(ServerContext serverContext, BatchProcessorContext batchProcessorContext) {
        sc = serverContext;
        logDir = Paths.get(serverContext.getServerConfig().get("--log-path").toString(), "log");
        openSegments = new ConcurrentHashMap<>();
        this.dataStore = new StreamLogDataStore(serverContext.getDataStore());

        initStreamLogDirectory();

        FileSystemConfig config = new FileSystemConfig(serverContext);
        this.fsAgent = FileSystemAgent.init(config, batchProcessorContext);

        // This is initialized to the size of the sum of all segment files
        logSizeQuota = FileSystemAgent.getResourceQuota();

        MicroMeterUtils.gauge(logUnitSizeMetricName + ".bytes", logSizeQuota,
                quota -> quota.getUsed().doubleValue());

        MicroMeterUtils.gauge(logUnitSizeMetricName + ".segments", openSegments, Map::size);
        currentTrimMark = MicroMeterUtils.gauge(logUnitTrimMarkMetricName, new AtomicLong(getTrimMark()));

        // Starting address initialization should happen before
        // initializing the tail segment (i.e. initializeMaxGlobalAddress)
        logMetadata = new LogMetadata(dataStore);
        initializeLogMetadata();

        // This can happen if a prefix trim happens on
        // addresses that haven't been written
        if (Math.max(logMetadata.getGlobalTail(), 0L) < getTrimMark()) {
            logMetadata.syncTailSegment(getTrimMark() - 1);
        }

    }

    private long getStartingSegment() {
        return dataStore.getStartingAddress() / RECORDS_PER_LOG_FILE;
    }

    /**
     * Create stream log directory if not exists
     */
    private void initStreamLogDirectory() {
        try {
            if (!logDir.toFile().exists()) {
                Files.createDirectories(logDir);
            }

            String corfuDir = logDir.getParent().toString();
            File corfuDirFile = new File(corfuDir);
            if (!corfuDirFile.canWrite()) {
                throw new LogUnitException("Corfu directory is not writable " + corfuDir);
            }

            File logDirectory = new File(logDir.toString());
            if (!logDirectory.canWrite()) {
                throw new LogUnitException("Stream log directory not writable in " + corfuDir);
            }
        } catch (IOException ioe) {
            throw new LogUnitException(ioe);
        }

        log.info("initStreamLogDirectory: initialized {}", logDir);
    }

    /**
     * This method will scan the log (i.e. read all log segment files)
     * on this LU and create a map of stream offsets and the global
     * addresses seen.
     * <p>
     * consecutive segments from [startSegment, endSegment]
     */
    private void initializeLogMetadata() {
        long startingSegment = getStartingSegment();
        long tailSegment = dataStore.getTailSegment();

        long start = System.currentTimeMillis();
        // Scan the log in reverse, this will ease stream trim mark resolution (as we require the
        // END records of a checkpoint which are always the last entry in this stream)
        // Note: if a checkpoint END record is not found (i.e., incomplete) this data is not considered
        // for stream trim mark computation.
        for (long currentSegment = tailSegment; currentSegment >= startingSegment; currentSegment--) {
            try (Segment segment = getSegmentHandleForAddress(currentSegment * RECORDS_PER_LOG_FILE + 1)) {
                for (Long address : segment.getAddresses()) {
                    // skip trimmed entries
                    if (address < dataStore.getStartingAddress()) {
                        continue;
                    }
                    LogData logEntry = read(address);
                    logMetadata.update(logEntry, true);
                }
            }
        }

        // Open segment will add entries to the writeChannels map, therefore we need to clear it
        openSegments.clear();
        long end = System.currentTimeMillis();
        log.info("initializeStreamTails: took {} ms to load {}, log start {}", end - start, logMetadata, getTrimMark());
    }


    @Override
    public boolean quotaExceeded() {
        return !logSizeQuota.hasAvailable();
    }

    @Override
    public long quotaLimitInBytes() {
        return logSizeQuota.getLimit();
    }

    @Override
    public long getLogTail() {
        return logMetadata.getGlobalTail();
    }

    @Override
    public TailsResponse getTails(List<UUID> streams) {
        Map<UUID, Long> tails = new HashMap<>();
        streams.forEach(stream -> {
            tails.put(stream, logMetadata.getStreamTails().get(stream));
        });
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public StreamsAddressResponse getStreamsAddressSpace() {
        return new StreamsAddressResponse(logMetadata.getGlobalTail(), logMetadata.getStreamsAddressSpaceMap());
    }

    @Override
    public TailsResponse getAllTails() {
        Map<UUID, Long> tails = new HashMap<>(logMetadata.getStreamTails());
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public long getCommittedTail() {
        return dataStore.getCommittedTail();
    }

    @Override
    public void updateCommittedTail(long committedTail) {
        dataStore.updateCommittedTail(committedTail);
    }

    @Override
    public void prefixTrim(long address) {
        if (isTrimmed(address)) {
            log.warn("prefixTrim: Ignoring repeated trim {}", address);
            return;
        }

        // TODO(Maithem): Although this operation is persisted to disk,
        // the startingAddress can be lost even after the method has completed.
        // This is due to the fact that updates on the local datastore don't
        // expose disk sync functionality.
        long newStartingAddress = address + 1;
        dataStore.updateStartingAddress(newStartingAddress);
        logMetadata.syncTailSegment(address);
        log.debug("Trimmed prefix, new address {}", newStartingAddress);
        currentTrimMark.ifPresent(counter -> counter.set(newStartingAddress));
        // Trim address space maps.
        logMetadata.prefixTrim(address);
    }

    private boolean isTrimmed(long address) {
        return address < dataStore.getStartingAddress();
    }

    @Override
    public void sync(boolean force) throws IOException {
        if (force) {
            Segment[] dirtySegments = openSegments.values().stream()
                    .filter(Segment::isDirty)
                    .toArray(Segment[]::new);

            Arrays.sort(dirtySegments, Comparator.comparingLong(o -> o.id));
            for (Segment sh : dirtySegments) {
                Optional<Timer.Sample> sample =
                        MicroMeterUtils.startTimer();
                sh.flush();
                MicroMeterUtils.time(sample, "logunit.fsync.timer");
                log.trace("Syncing segment {}", sh.id);
            }
        }
    }

    @Override
    public synchronized void compact() {
        Lock lock = resetLock.writeLock();
        lock.lock();
        try {
            trimPrefix();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getTrimMark() {
        return dataStore.getStartingAddress();
    }

    private void trimPrefix() {
        // Trim all segments up till the segment that contains the starting address
        // (i.e. trim only complete segments)
        long endSegment = getStartingSegment() - 1;

        if (endSegment < 0) {
            log.debug("Only one segment detected, ignoring trim");
            return;
        }

        // Close segments before deleting their corresponding log files
        closeSegmentHandlers(endSegment);

        deleteFilesMatchingFilter(file -> {
            try {
                String segmentStr = file.getName().split("\\.")[0];
                return Long.parseLong(segmentStr) <= endSegment;
            } catch (Exception e) {
                log.warn("trimPrefix: ignoring file {}", file.getName());
                return false;
            }
        });

        log.info("trimPrefix: completed, end segment {}", endSegment);
    }

    Segment getSegmentHandleForSegment(long segmentId) {
        Segment handle = openSegments.computeIfAbsent(segmentId,
                a -> new Segment(a, RECORDS_PER_LOG_FILE, logDir, logSizeQuota));
        handle.retain();
        return handle;
    }

    /**
     * Return a SegmentHandle for a corresponding log address.
     *
     * @param address global log address.
     * @return The corresponding segment for address
     */
    Segment getSegmentHandleForAddress(long address) {
        long segmentId = address / RECORDS_PER_LOG_FILE;
        return getSegmentHandleForSegment(segmentId);
    }


    /**
     * This method requests for known addresses in this Log Unit in the specified consecutive
     * range of addresses.
     *
     * @param rangeStart Start address of range.
     * @param rangeEnd   End address of range.
     * @return Set of known addresses.
     */
    @Override
    public Set<Long> getKnownAddressesInRange(long rangeStart, long rangeEnd) {

        Lock lock = resetLock.readLock();
        lock.lock();

        try {
            Set<Long> result = new HashSet<>();
            for (long address = rangeStart; address <= rangeEnd; address++) {
                Segment handle = getSegmentHandleForAddress(address);
                if (handle.contains(address)) {
                    result.add(address);
                }
                handle.release();
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Write a range of log data entries. This range write is used by state transfer. Three constraints
     * are checked, the range doesn't span to segments, the entries are sorted and "dense" (no holes) and
     * that non of the entries have been trimmed.
     *
     * @param range to write
     * @return pruned list of entries that doesn't contain any locally trimmed entries
     */
    private List<LogData> prepareRange(List<LogData> range) {
        Preconditions.checkArgument(!range.isEmpty(), "empty range!");

        long firstAddressInRange = range.get(0).getGlobalAddress();
        long lastAddressInRange = range.get(range.size() - 1).getGlobalAddress();

        Preconditions.checkArgument(firstAddressInRange <= lastAddressInRange, "range not sorted!");

        List<LogData> rangeToWrite = new ArrayList<>();
        long prevAddress = -1;
        for (int idx = 0; idx < range.size(); idx++) {
            LogData ld = range.get(idx);

            if (ld.isTrimmed()) {
                throw new OverwriteException(OverwriteCause.SAME_DATA);
            }

            if (prevAddress != -1) {
                Preconditions.checkArgument(prevAddress + 1 == ld.getGlobalAddress());
            }

            prevAddress = ld.getGlobalAddress();

            // Since state transfer cannot read trimmed addresses, we must make sure we aren't receiving any
            Preconditions.checkArgument(ld.isHole() || ld.isData());

            // since State sync and trim are two asynchronous process, we need to check if the incoming writes
            // are less than the local trim mark
            if (isTrimmed(ld.getGlobalAddress())) {
                continue;
            }

            rangeToWrite.add(ld);
        }

        return rangeToWrite;
    }

    private long getSegmentId(long address) {
        return address / RECORDS_PER_LOG_FILE;
    }

    @Override
    public void append(List<LogData> range) {

        List<LogData> rangeToWrite = prepareRange(range);

        if (rangeToWrite.isEmpty()) {
            return;
        }

        Map<Long, List<LogData>> batches = new HashMap<>(2);
        for (LogData ld : rangeToWrite) {
            batches.computeIfAbsent(getSegmentId(ld.getGlobalAddress()), i -> new ArrayList<>()).add(ld);
        }

        Preconditions.checkArgument(!batches.isEmpty() && batches.size() <= 2, "range too big!");

        for (long batchSegment : batches.keySet()) {
            Segment sh = getSegmentHandleForAddress(batchSegment * RECORDS_PER_LOG_FILE);
            for (LogData ld : batches.get(batchSegment)) {
                if (sh.contains(ld.getGlobalAddress())) {
                    sh.release();
                    throw new OverwriteException(OverwriteCause.SAME_DATA);
                }
            }
            sh.release();
        }

        try {

            long numBytes = 0;
            for (long batchSegment : batches.keySet()) {
                List<LogData> entries = batches.get(batchSegment);
                Segment sh = getSegmentHandleForAddress(entries.get(0).getGlobalAddress());
                numBytes += sh.write(entries);
                sh.release();
                logMetadata.syncTailSegment(entries.get(entries.size() - 1).getGlobalAddress());
                logMetadata.update(entries);
            }

            MicroMeterUtils.measure(numBytes, "logunit.write.throughput");
        } catch (IOException e) {
            log.error("Disk_write[{}-{}]: Exception", range.get(0).getGlobalAddress(),
                    range.get(range.size() - 1).getGlobalAddress(), e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public void append(long address, LogData entry) {
        if (isTrimmed(address)) {
            throw new OverwriteException(OverwriteCause.TRIM);
        }

        Segment segment = getSegmentHandleForAddress(address);

        try {
            if (segment.contains(address)) {
                OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
                log.trace("Disk_write[{}]: overwritten exception, cause: {}", address, overwriteCause);
                throw new OverwriteException(overwriteCause);
            } else {
                long size = segment.write(address, entry);
                logMetadata.syncTailSegment(address);
                logMetadata.update(entry, false);

                MicroMeterUtils.measure(size, "logunit.write.throughput");
            }
            log.trace("Disk_write[{}]: Written to disk.", address);
        } catch (IOException e) {
            log.error("Disk_write[{}]: Exception", address, e);
            throw new RuntimeException(e);
        } finally {
            segment.release();
        }
    }

    @Override
    public LogData read(long address) {
        Lock lock = resetLock.readLock();
        lock.lock();

        try {
            if (isTrimmed(address)) {
                return LogData.getTrimmed(address);
            }
            Segment segment = getSegmentHandleForAddress(address);

            try {
                return segment.read(address);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                segment.release();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(long address) throws TrimmedException {
        // auto commit client is expected to get TrimmedException and
        // retry as this indicates commit counter is falling behind.
        if (isTrimmed(address)) {
            throw new TrimmedException();
        }

        if (address <= getCommittedTail()) {
            return true;
        }

        Lock lock = resetLock.readLock();
        lock.lock();

        Segment segment = getSegmentHandleForAddress(address);

        try {
            return segment.contains(address);
        } finally {
            segment.release();
            lock.unlock();
        }
    }

    @Override
    public void close() {
        fsAgent.shutdown();
        for (Segment fh : openSegments.values()) {
            fh.close();
        }
        openSegments = new ConcurrentHashMap<>();
        removeLocalGauges();
    }

    /**
     * Closes all segment handlers up to and including the handler for the endSegment.
     *
     * @param endSegment The segment index of the last segment up to (including) the end segment.
     */
    @VisibleForTesting
    void closeSegmentHandlers(long endSegment) {
        for (Segment sh : openSegments.values()) {
            if (sh.id <= endSegment) {
                sh.close();
                Segment temp = openSegments.remove(sh.id);
                Preconditions.checkState(temp != null);
            }
        }
    }

    private void closeAllSegmentHandlers() {
        for (Segment sh : openSegments.values()) {
            sh.close();
            openSegments.remove(sh.id);
        }
    }

    /**
     * Deletes all files matching the given filter.
     *
     * @param fileFilter File filter to delete files.
     */
    private void deleteFilesMatchingFilter(FileFilter fileFilter) {
        int numFiles = 0;
        long freedBytes = 0;
        File dir = logDir.toFile();
        File[] files = dir.listFiles(fileFilter);
        if (files == null) {
            return;
        }

        for (File file : files) {
            long delta = file.length();

            if (!file.delete()) {
                log.error("deleteFilesMatchingFilter: Couldn't delete file {}", file.getName());
            } else {
                freedBytes += delta;
                numFiles++;
            }
        }
        logSizeQuota.release(freedBytes);
        log.info("deleteFilesMatchingFilter: completed, deleted {} files, freed {} bytes", numFiles, freedBytes);
    }

    /**
     * TODO(Maithem) remove this method. Obtaining a new instance should happen
     * through instantiation not by clearing this class' state
     * <p>
     * Resets the Stream log.
     * Clears all data and resets the handlers.
     * Usage: To heal a recovering node, we require to wipe off existing data.
     */
    @Override
    public void reset() {
        // Trim all segments
        log.warn("Reset. Global Tail:{}", logMetadata.getGlobalTail());

        Lock lock = resetLock.writeLock();
        lock.lock();

        try {
            long committedTail = getCommittedTail();
            long latestSegment = getSegmentId(getLogTail());
            long committedTailSegment = getSegmentId(committedTail);
            for (long currSegmentId = committedTailSegment; currSegmentId <= latestSegment; currSegmentId++) {
                // Close segments before deleting their corresponding log files
                String segmentFilePath;
                try (Segment sh = getSegmentHandleForSegment(currSegmentId)) {
                    openSegments.remove(sh.id);
                    segmentFilePath = sh.segmentFilePath;
                }

                deleteFilesMatchingFilter(file -> file.getAbsolutePath().equals(segmentFilePath));
            }

            long newTailAddress = StreamLogDataStore.ZERO_ADDRESS;
            if (committedTailSegment > 0) {
                newTailAddress = committedTailSegment * RECORDS_PER_LOG_FILE + 1;
            }

            logMetadata = new LogMetadata(dataStore);
            logMetadata.syncTailSegment(newTailAddress);
            initializeLogMetadata();
            // would this lose the gauges pre-reset?
            removeLocalGauges();
        } finally {
            log.info("reset: Finished");
            lock.unlock();
        }
    }

    private void removeLocalGauges() {
        MicroMeterUtils.removeGaugesWithNoTags(
                logUnitSizeMetricName + ".bytes",
                logUnitSizeMetricName + ".segments",
                logUnitTrimMarkMetricName);
    }

    @VisibleForTesting
    Collection<Segment> getOpenSegments() {
        return openSegments.values();
    }

    @VisibleForTesting
    Collection<Segment> getDirtySegments() {
        return openSegments.values().stream()
                .filter(Segment::isDirty)
                .collect(Collectors.toSet());
    }
}
