package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.FileSystemAgent.PartitionAgent.PartitionAttribute;
import org.corfudb.runtime.exceptions.LogUnitException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Slf4j
public final class FileSystemAgent {
    private static Optional<FileSystemAgent> instance = Optional.empty();

    private final FileSystemConfig config;
    // Resource quota to track the log size
    @Getter
    private final ResourceQuota logSizeQuota;

    @Getter
    private final PartitionAttribute partitionAttribute;

    private FileSystemAgent(FileSystemConfig config) {
        this.config = config;

        long initialLogSize = estimateSize();
        long logSizeLimit = getLogSizeLimit();

        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        partitionAttribute = new PartitionAgent(config).getPartitionAttribute();

        logSizeQuota.consume(initialLogSize);
        log.info("StreamLogFiles: {} size is {} bytes, limit {}", config.logDir, initialLogSize, logSizeLimit);
    }

    private long getLogSizeLimit() {
        long fileSystemCapacity = getFileSystemCapacity();

        // Derived size in bytes that normal writes to the log unit are capped at.
        // This is derived as a percentage of the log's filesystem capacity.
        return (long) (fileSystemCapacity * config.limitPercentage / 100);
    }

    public static void init(FileSystemConfig config) {
        instance = Optional.of(new FileSystemAgent(config));
    }

    public static boolean configured(){
        return instance.isPresent();
    }

    public static ResourceQuota getResourceQuota() {
        Supplier<IllegalStateException> err = () -> new IllegalStateException("FileSystemAgent not configured");
        return instance.orElseThrow(err).logSizeQuota;
    }

    public static PartitionAttribute getPartition() {
        Supplier<IllegalStateException> err = () -> new IllegalStateException("FileSystemAgent not configured");
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

    private BasicFileAttributes setFileAttributes() {
        BasicFileAttributeView fileAttributeView = Files.getFileAttributeView(
                config.logDir, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        BasicFileAttributes basicFileAttributes = null;
        try {
            basicFileAttributes = fileAttributeView.readAttributes();
        } catch (Exception e) {
            log.error("getPartitionStats: Error while reading from filesystem", e);
        }
        return basicFileAttributes;
    }

    public static class FileSystemConfig {
        private final Path logDir;
        private final double limitPercentage;

        public FileSystemConfig(ServerContext serverContext) {
            String limitParam = serverContext.getServerConfig(String.class, "--log-size-quota-percentage");
            limitPercentage = Double.parseDouble(limitParam);

            checkLimits();

            String logPath = serverContext.getServerConfig(String.class, "--log-path");
            logDir = Paths.get(logPath, "log");
        }

        public FileSystemConfig(Path logDir, double limitPercentage) {
            this.logDir = logDir;
            this.limitPercentage = limitPercentage;
            checkLimits();
        }

        private void checkLimits() {
            if (limitPercentage < 0.0 || limitPercentage > 100.0) {
                String msg = String.format("Invalid quota: quota(%f)%% must be between 0-100%%", limitPercentage);
                throw new LogUnitException(msg);
            }
        }
    }

    public static class PartitionAgent {
        @Getter
        private PartitionAttribute partitionAttribute;
        private final Path logPartition;
        private final FileSystemConfig config;
        public PartitionAgent(FileSystemConfig config) {
            this.config =  config;
            logPartition = Paths.get(config.logDir.getRoot().toString(), config.logDir.subpath(0, 1).toString());
            partitionAttribute = setPartitionAttribute();
            //we need a scheduler
        }

        private PartitionAttribute setPartitionAttribute() {
            try {
                File logDirectoryFile = config.logDir.toFile();
                FileStore fileStore = Files.getFileStore(config.logDir);
                partitionAttribute = new PartitionAttribute(
                        fileStore.isReadOnly() || !logDirectoryFile.canWrite(),
                        fileStore.getUsableSpace(),
                        fileStore.getTotalSpace()
                );
            } catch (IOException e) {
                log.error("setPartitionStats: Error while getting PartitionStats", e);
            }

            return partitionAttribute;
        }

        public PartitionAttribute resetPartitionInfo() {
            partitionAttribute = setPartitionAttribute();
            return partitionAttribute;
        }

        @AllArgsConstructor
        @Getter
        public static class PartitionAttribute {
            private final boolean readOnly;
            private final long availableSpace;
            private final long totalSpace;
        }
    }
}
