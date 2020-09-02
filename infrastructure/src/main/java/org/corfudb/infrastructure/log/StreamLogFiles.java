package org.corfudb.infrastructure.log;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.store.Database;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */
@Slf4j
public class StreamLogFiles implements StreamLog {

  public static final int VERSION = 3;

  private final Path logDir;

  private final StreamLogDataStore dataStore;

  // =================Log Metadata=================
  // TODO(Maithem) this should effectively be final, but it is used
  // by a reset API that clears the state of this class, on reset
  // a new instance of this class should be created after deleting
  // the files of the old instance
  private LogMetadata logMetadata;

  // Derived size in bytes that normal writes to the log unit are capped at.
  // This is derived as a percentage of the log's filesystem capacity.
  private final long logSizeLimit;

  // Resource quota to track the log size
  private ResourceQuota logSizeQuota;

  /**
   * Prevents corfu from reading and executing maintenance operations (reset log unit and stream log
   * compaction) in parallel
   */
  private final ReadWriteLock resetLock = new ReentrantReadWriteLock();

  private final Database database;

  /**
   * Returns a file-based stream log object.
   *
   * @param serverContext Context object that provides server state such as epoch, segment and start
   *     address
   */
  public StreamLogFiles(ServerContext serverContext) {
    logDir = Paths.get(serverContext.getServerConfig().get("--log-path").toString(), "log");
    this.dataStore = new StreamLogDataStore(serverContext.getDataStore());

    String logSizeLimitPercentageParam =
        (String) serverContext.getServerConfig().get("--log-size-quota-percentage");
    final double logSizeLimitPercentage = Double.parseDouble(logSizeLimitPercentageParam);
    if (logSizeLimitPercentage < 0.0 || 100.0 < logSizeLimitPercentage) {
      String msg =
          String.format(
              "Invalid quota: quota(%f)%% must be between 0-100%%", logSizeLimitPercentage);
      throw new LogUnitException(msg);
    }

    long maxDatabaseSize = 1024L * 1024L * 1024L * 50L;
    this.database = new Database(logDir, maxDatabaseSize, PooledByteBufAllocator.DEFAULT, false);

    long fileSystemCapacity = initStreamLogDirectory();
    logSizeLimit = (long) (fileSystemCapacity * logSizeLimitPercentage / 100.0);

    long initialLogSize = estimateSize(logDir);
    log.info("StreamLogFiles: {} size is {} bytes, limit {}", logDir, initialLogSize, logSizeLimit);
    logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
    logSizeQuota.consume(initialLogSize);

    // Starting address initialization should happen before
    // initializing the tail segment (i.e. initializeMaxGlobalAddress)
    logMetadata = new LogMetadata();
    initializeLogMetadata();
  }

  /**
   * Create stream log directory if not exists
   *
   * @return total capacity of the file system that owns the log files.
   */
  private long initStreamLogDirectory() {
    long fileSystemCapacity;

    try {
      if (!logDir.toFile().exists()) {
        Files.createDirectories(logDir);
      }

      String corfuDir = logDir.getParent().toString();
      FileStore corfuDirBackend = Files.getFileStore(Paths.get(corfuDir));

      File corfuDirFile = new File(corfuDir);
      if (!corfuDirFile.canWrite()) {
        throw new LogUnitException("Corfu directory is not writable " + corfuDir);
      }

      File logDirectory = new File(logDir.toString());
      if (!logDirectory.canWrite()) {
        throw new LogUnitException("Stream log directory not writable in " + corfuDir);
      }

      fileSystemCapacity = corfuDirBackend.getTotalSpace();
    } catch (IOException ioe) {
      throw new LogUnitException(ioe);
    }

    log.info("initStreamLogDirectory: initialized {}", logDir);
    return fileSystemCapacity;
  }

  /**
   * This method will scan the log (i.e. read all log segment files) on this LU and create a map of
   * stream offsets and the global addresses seen.
   *
   * <p>consecutive segments from [startSegment, endSegment]
   */
  private void initializeLogMetadata() {

    long start = System.currentTimeMillis();
    // Scan the log in reverse, this will ease stream trim mark resolution (as we require the
    // END records of a checkpoint which are always the last entry in this stream)
    // Note: if a checkpoint END record is not found (i.e., incomplete) this data is not considered
    // for stream trim mark computation.

    database.reverseIter(ld -> logMetadata.update(ld, true), dataStore.getStartingAddress());

    // Open segment will add entries to the writeChannels map, therefore we need to clear it
    long end = System.currentTimeMillis();
    log.info(
        "initializeStreamTails: took {} ms to load {}, log start {}",
        end - start,
        logMetadata,
        getTrimMark());
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
    streams.forEach(
        stream -> {
          tails.put(stream, logMetadata.getStreamTails().get(stream));
        });
    return new TailsResponse(logMetadata.getGlobalTail(), tails);
  }

  @Override
  public StreamsAddressResponse getStreamsAddressSpace() {
    return new StreamsAddressResponse(
        logMetadata.getGlobalTail(), logMetadata.getStreamsAddressSpaceMap());
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
    log.debug("Trimmed prefix, new starting address {}", newStartingAddress);

    // Trim address space maps.
    logMetadata.prefixTrim(address);
  }

  private boolean isTrimmed(long address) {
    return address < dataStore.getStartingAddress();
  }

  @Override
  public void flush() {
    database.sync();
  }

  @Override
  public synchronized void compact() {
    Lock lock = resetLock.writeLock();
    lock.lock();
    try {
      database.trim(dataStore.getStartingAddress() - 1);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long getTrimMark() {
    return dataStore.getStartingAddress();
  }

  // updateGlobalTail
  // logMetadata.update(entries);
  // logSizeQuota.consume(buf.remaining());

  /**
   * This method requests for known addresses in this Log Unit in the specified consecutive range of
   * addresses.
   *
   * @param rangeStart Start address of range.
   * @param rangeEnd End address of range.
   * @return Set of known addresses.
   */
  @Override
  public Set<Long> getKnownAddressesInRange(long rangeStart, long rangeEnd) {

    Lock lock = resetLock.readLock();
    lock.lock();

    try {
      Set<Long> result = new HashSet<>();
      for (long address = rangeStart; address <= rangeEnd; address++) {
        if (database.read(address) != null) {
          result.add(address);
        }
      }
      return result;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void append(List<LogData> batch) {

    if (batch.isEmpty()) {
      log.info("No entries to write.");
      return;
    }

    for (LogData ld : batch) {
      Objects.requireNonNull(ld);
      checkState(!ld.isEmpty());
      checkState(!ld.isTrimmed());
      if (isTrimmed(ld.getGlobalAddress())) {
        throw new OverwriteException(OverwriteCause.TRIM);
      }

      if (database.read(ld.getGlobalAddress()) != null) {
        throw new OverwriteException(OverwriteCause.DIFF_DATA);
      }
    }

    database.append(new HashSet<>(batch));
    logMetadata.update(batch);
  }

  @Override
  public void append(LogData entry) {
    append(Collections.singletonList(entry));
  }

    /**
     * try { // make sure the entry doesn't currently exist... // (probably need a faster way to do
     * this - high watermark?) if (segment.getKnownAddresses().containsKey(address) ||
     * segment.getTrimmedAddresses().contains(address)) { if (entry.getRank() == null) {
     * OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
     * log.trace("Disk_write[{}]: overwritten exception, cause: {}", address, overwriteCause); throw
     * new OverwriteException(overwriteCause); } else { // the method below might throw
     * DataOutrankedException or ValueAdoptedException assertAppendPermittedUnsafe(address, entry);
     * AddressMetaData addressMetaData = writeRecord(segment, address, entry);
     * segment.getKnownAddresses().put(address, addressMetaData); } } else { AddressMetaData
     * addressMetaData = writeRecord(segment, address, entry);
     * segment.getKnownAddresses().put(address, addressMetaData); } log.trace("Disk_write[{}]:
     * Written to disk.", address);
     */

  @Override
  public LogData read(long address) {
    Lock lock = resetLock.readLock();
    lock.lock();

    try {
      if (isTrimmed(address)) {
        return LogData.getTrimmed(address);
      }

      return database.read(address);

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

    try {
      return database.read(address) != null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    database.close();
  }

  // On trim update quota

  /**
   * TODO(Maithem) remove this method. Obtaining a new instance should happen through instantiation
   * not by clearing this class' state
   *
   * <p>Resets the Stream log. Clears all data and resets the handlers. Usage: To heal a recovering
   * node, we require to wipe off existing data.
   */
  @Override
  public void reset() {
    // Trim all segments
    log.warn("Reset. Global Tail:{}", logMetadata.getGlobalTail());

    Lock lock = resetLock.writeLock();
    lock.lock();

    try {

      dataStore.resetStartingAddress();
      logMetadata = new LogMetadata();
      // TODO(Maithem): nuke database
      database.trim(Long.MAX_VALUE);

      logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
      log.info("reset: Completed");
    } finally {
      lock.unlock();
    }
  }

  public static class Checksum {

    private Checksum() {
      // prevent creating instances
    }

    /**
     * Returns checksum used for log.
     *
     * @param bytes data over which to compute the checksum
     * @return checksum of bytes
     */
    public static int getChecksum(byte[] bytes) {
      Hasher hasher = Hashing.crc32c().newHasher();
      for (byte a : bytes) {
        hasher.putByte(a);
      }

      return hasher.hash().asInt();
    }

    public static int getChecksum(int num) {
      Hasher hasher = Hashing.crc32c().newHasher();
      return hasher.putInt(num).hash().asInt();
    }
  }

  /** Estimate the size (in bytes) of a directory. From https://stackoverflow.com/a/19869323 */
  @VisibleForTesting
  static long estimateSize(Path directoryPath) {
    final AtomicLong size = new AtomicLong(0);
    try {
      Files.walkFileTree(
          directoryPath,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              size.addAndGet(attrs.size());
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
              // Skip folders that can't be traversed
              log.error("skipped: {}", file, exc);
              return FileVisitResult.CONTINUE;
            }
          });

      return size.get();
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
  }
}
