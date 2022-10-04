package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.log.FileSystemAgent.PartitionAgent.PartitionAttribute;
import org.corfudb.infrastructure.log.StreamLog.PersistenceMode;
import org.corfudb.runtime.exceptions.LogUnitException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.corfudb.infrastructure.health.Component.LOG_UNIT;

@Slf4j
public final class FileSystemAgent {
    private static final String NOT_CONFIGURED_ERR_MSG = "FileSystemAgent not configured";
    private static final int INIT_DELAY = 0;
    private static final int DELAY_NUM = 1;
    private static final TimeUnit DELAY_UNITS = SECONDS;

    private static Optional<FileSystemAgent> instance = Optional.empty();

    private final FileSystemConfig config;
    // Resource quota to track the log size

    private final ResourceQuota logSizeQuota;

    private final PartitionAttribute partitionAttribute;

    @Getter
    private final ScheduledExecutorService scheduler;

    private FileSystemAgent(FileSystemConfig config) {
        this.config = config;

        long initialLogSize;
        long logSizeLimit;
        if (config.mode == PersistenceMode.MEMORY) {
            initialLogSize = 0;
            logSizeLimit = Long.MAX_VALUE;

            partitionAttribute = new PartitionAttribute(false, Long.MAX_VALUE, Long.MAX_VALUE);
        } else {
            initialLogSize = estimateSize();
            logSizeLimit = getLogSizeLimit();

            partitionAttribute = new PartitionAgent(config).getPartitionAttribute();
        }

        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        logSizeQuota.consume(initialLogSize);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(
                this::reportQuotaExceeded, INIT_DELAY, DELAY_NUM, DELAY_UNITS);
        log.info("FileSystemAgent: {} size is {} bytes, limit {}", config.logDir, initialLogSize, logSizeLimit);
    }

    private long getLogSizeLimit() {
        long fileSystemCapacity = getFileSystemCapacity();

        // Derived size in bytes that normal writes to the log unit are capped at.
        // This is derived as a percentage of the log's filesystem capacity.
        return (long) (fileSystemCapacity * config.limitPercentage / 100);
    }

    private void reportQuotaExceeded() {
        try {
            Issue issue = Issue.createIssue(LOG_UNIT, Issue.IssueId.QUOTA_EXCEEDED_ERROR, "Quota exceeded");
            if (!getResourceQuota().hasAvailable()) {
                HealthMonitor.reportIssue(issue);
            } else {
                HealthMonitor.resolveIssue(issue);
            }
        } catch (Exception e) {
            log.error("Exception in quota monitor:", e);
        }

    }

    public static void init(FileSystemConfig config) {
        instance = Optional.of(new FileSystemAgent(config));
    }

    public static void shutdown() {
        PartitionAgent.shutdown();
        instance.ifPresent(i -> i.getScheduler().shutdown());
    }

    public static ResourceQuota getResourceQuota() {
        Supplier<IllegalStateException> err = () -> new IllegalStateException(NOT_CONFIGURED_ERR_MSG);
        return instance.orElseThrow(err).logSizeQuota;
    }

    public static PartitionAttribute getPartitionAttribute() {
        Supplier<IllegalStateException> err = () -> new IllegalStateException(NOT_CONFIGURED_ERR_MSG);
        return instance.orElseThrow(err).partitionAttribute;
    }

    /**
     * Estimate the size (in bytes) of a directory.
     * From https://stackoverflow.com/a/19869323
     */
    @VisibleForTesting
    long estimateSize() {
        final AtomicLong size = new AtomicLong(0);
        try {
            Files.walkFileTree(config.logDir, new SimpleFileVisitor<Path>() {
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

    /**
     * Get corfu log dir partition size
     *
     * @return total capacity of the file system that owns the log files.
     */
    private long getFileSystemCapacity() {
        Path corfuDir = config.logDir.getParent();
        try {
            return Files.getFileStore(corfuDir).getTotalSpace();
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading corfu log directory, path: " + corfuDir, e);
        }
    }

    public static class FileSystemConfig {
        private final Path logDir;
        private final double limitPercentage;
        private final PersistenceMode mode;

        public FileSystemConfig(ServerContext serverContext) {
            String limitParam = serverContext.getServerConfig(String.class, "--log-size-quota-percentage");
            limitPercentage = Double.parseDouble(limitParam);

            checkLimits();

            String logPath = serverContext.getServerConfig(String.class, "--log-path");
            logDir = Paths.get(logPath, "log");

            LogUnitServerConfig luConfig = LogUnitServerConfig.parse(serverContext.getServerConfig());
            mode = PersistenceMode.fromBool(luConfig.isMemoryMode());
        }

        public FileSystemConfig(Path logDir, double limitPercentage, PersistenceMode mode) {
            this.logDir = logDir;
            this.limitPercentage = limitPercentage;
            this.mode = mode;
            checkLimits();
        }

        private void checkLimits() {
            if (limitPercentage < 0.0 || limitPercentage > 100.0) {
                String msg = String.format("Invalid quota: quota(%f)%% must be between 0-100%%", limitPercentage);
                throw new LogUnitException(msg);
            }
        }
    }

    /**
     * This class provides resources required for PartitionAttribute and its usages.
     * PartitionAttribute has attributes related to the partition containing the log files like
     * readOnly, availableSpace and totalSpace that are refreshed every
     * {@link PartitionAttribute#UPDATE_INTERVAL} seconds using the
     * {@link PartitionAttribute#scheduler}.
     */
    public static class PartitionAgent {

        // Interval when the PartitionAttribute values are reset by the scheduler
        private static final int UPDATE_INTERVAL = 5;

        // We don't need any delay for the scheduler
        private static final int NO_DELAY = 0;

        // This contains the attribute values of the log partition
        @Getter
        private volatile PartitionAttribute partitionAttribute;

        // Path of the log partition, for example /config
        private final Path logPartition;
        private final FileSystemConfig config;

        // A single thread scheduler that has a single instance of execution at any given time.
        private static final ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        private static ScheduledFuture<?> scheduledFuture = null;

        public PartitionAgent(FileSystemConfig config) {
            this.config = config;
            // Joins root directory with its first sub path to get log partition.
            // For example, "/" + "config"
            logPartition = Paths.get(config.logDir.getRoot().toString(),
                    config.logDir.subpath(0, 1).toString());
            initializeScheduler();
            setPartitionAttribute();
        }

        /**
         * Resets PartitionAttribute's fields every {@link PartitionAttribute#UPDATE_INTERVAL}
         * seconds after the previous set task is completed.
         */
        private void initializeScheduler() {
            scheduledFuture = scheduler.scheduleWithFixedDelay(
                    this::setPartitionAttribute, NO_DELAY, UPDATE_INTERVAL, SECONDS
            );
        }

        /**
         * Sets PartitionAttribute's fields with the values from log file and the log partition.
         */
        private void setPartitionAttribute() {
            log.info("setPartitionAttribute: fetching PartitionAttribute.");
            try {
                // Log path to check if it is in readOnly mode
                File logDirectoryFile = config.logDir.toFile();

                // Partition of the log to fetch total and available space, and check readOnly
                FileStore fileStore = Files.getFileStore(logPartition);
                partitionAttribute = new PartitionAttribute(
                        fileStore.isReadOnly() || !logDirectoryFile.canWrite(),
                        fileStore.getUsableSpace(),
                        fileStore.getTotalSpace()
                );
                log.info("setPartitionAttribute: fetched PartitionAttribute successfully. " +
                        "{}", partitionAttribute);

            } catch (Exception ex) {
                log.error("setPartitionAttribute: Error while fetching PartitionAttributes.", ex);
            }
        }

        /**
         * This class contains the current state of the log partition.
         * Its values are reset every {@link PartitionAttribute#UPDATE_INTERVAL} seconds
         * using the {@link PartitionAttribute#scheduler}.
         */
        @AllArgsConstructor
        @Getter
        @ToString
        public static class PartitionAttribute {
            private final boolean readOnly;
            private final long availableSpace;
            private final long totalSpace;
        }

        /**
         * Clean up
         */
        public static void shutdown() {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        }
    }
}
