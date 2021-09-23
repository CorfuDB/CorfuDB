package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.exceptions.LogUnitException;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Slf4j
public class FileSystemAgent {
    private static Optional<FileSystemAgent> INSTANCE = Optional.empty();

    private final ResourceQuotaConfig config;
    // Resource quota to track the log size
    @Getter
    private final ResourceQuota logSizeQuota;

    private FileSystemAgent(ResourceQuotaConfig config) {
        this.config = config;

        long initialLogSize = estimateSize();
        long logSizeLimit = getLogSizeLimit();

        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        logSizeQuota.consume(initialLogSize);
        log.info("StreamLogFiles: {} size is {} bytes, limit {}", config.logDir, initialLogSize, logSizeLimit);
    }

    private long getLogSizeLimit() {
        long fileSystemCapacity = getFileSystemCapacity();

        // Derived size in bytes that normal writes to the log unit are capped at.
        // This is derived as a percentage of the log's filesystem capacity.
        return (long) (fileSystemCapacity * config.limitPercentage / 100);
    }

    public static void init(ResourceQuotaConfig config) {
        INSTANCE = Optional.of(new FileSystemAgent(config));
    }

    public static boolean configured(){
        return INSTANCE.isPresent();
    }

    public static ResourceQuota getResourceQuota() {
        Supplier<IllegalStateException> err = () -> new IllegalStateException("ResourceQuota not configured");
        return INSTANCE.orElseThrow(err).logSizeQuota;
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

    public static class ResourceQuotaConfig {
        private final Path logDir;
        private final double limitPercentage;

        public ResourceQuotaConfig(ServerContext serverContext) {
            String limitParam = serverContext.getServerConfig(String.class, "--log-size-quota-percentage");
            limitPercentage = Double.parseDouble(limitParam);

            checkLimits();

            String logPath = serverContext.getServerConfig(String.class, "--log-path");
            logDir = Paths.get(logPath, "log");
        }

        public ResourceQuotaConfig(Path logDir, double limitPercentage) {
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
}
