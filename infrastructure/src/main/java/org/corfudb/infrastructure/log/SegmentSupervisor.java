package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * The class manages corfu's file segments
 */
@Builder
@Slf4j
public class SegmentSupervisor {
    @Getter
    private final ConcurrentMap<Path, SegmentHandle> channels = new ConcurrentHashMap<>();
    private final Set<FileChannel> channelsToSync = ConcurrentHashMap.newKeySet();

    @NonNull
    private final Path logDir;

    /**
     * Gets the file channel for a particular address, creating it
     * if is not present in the map.
     *
     * @param address The address to open.
     * @return The FileChannel for that address.
     */
    @VisibleForTesting
    SegmentHandle getSegmentByAddress(long address, AddressSpaceLoader addressSpaceLoader) {
        long segmentId = address / StreamLogFiles.RECORDS_PER_LOG_FILE;

        Path filePath = logDir.resolve(segmentId + ".log");

        SegmentHandle handle = channels.computeIfAbsent(filePath, missingFilePath -> {
            FileChannel writeCh = null;
            FileChannel readCh = null;

            try {
                writeCh = getChannel(missingFilePath, false);
                readCh = getChannel(missingFilePath, true);

                SegmentHandle segment = new SegmentHandle(segmentId, writeCh, readCh, missingFilePath);
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                // Once the segment address space is loaded, it should be ready to accept writes.
                addressSpaceLoader.load(segment);
                return segment;
            } catch (IOException e) {
                log.error("Error opening file {}", missingFilePath, e);
                IOUtils.closeQuietly(writeCh);
                IOUtils.closeQuietly(readCh);
                throw new IllegalStateException(e);
            } catch (RuntimeException ex) {
                //Prevents file resources leaks in case of any RuntimeException.
                IOUtils.closeQuietly(writeCh);
                IOUtils.closeQuietly(readCh);
                throw ex;
            }
        });

        handle.retain();
        return handle;
    }

    @Nullable
    private FileChannel getChannel(Path filePath, boolean readOnly) throws IOException {
        if (readOnly) {
            if (!filePath.toFile().exists()) {
                throw new FileNotFoundException(filePath.toString());
            }

            return FileChannel.open(filePath, EnumSet.of(StandardOpenOption.READ));
        }

        EnumSet<StandardOpenOption> options = EnumSet.of(
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
        FileChannel channel = FileChannel.open(filePath, options);

        // First time creating this segment file, need to sync the parent directory
        File segFile = filePath.toFile();
        syncDirectory(segFile.getParent());
        return channel;
    }

    /**
     * Close a segment
     *
     * @param segment segment handler
     */
    public void close(SegmentHandle segment) {
        if (segment.getRefCount() != 0) {
            log.warn("closeSegmentHandlers: Segment {} is trimmed, but refCount is {}, attempting to trim anyways",
                    segment.getSegment(), segment.getRefCount()
            );
        }

        channelsToSync.remove(segment.getWriteChannel());
        channels.remove(segment.getFileName(), segment);
        segment.close();
    }

    void sync(boolean force) throws IOException {
        Optional<IOException> err = Optional.empty();

        if (force) {
            for (FileChannel ch : channelsToSync) {
                try {
                    ch.force(true);
                } catch (IOException ex) {
                    log.warn("Error - can't sync a file channel");
                    err = Optional.of(ex);
                } finally {
                    channelsToSync.remove(ch);
                }
            }
        }
        log.trace("Sync'd {} channels", channelsToSync.size());

        if (err.isPresent()) {
            throw err.get();
        }
    }

    /**
     * Closes all segment handlers up to and including the handler for the endSegment.
     *
     * @param endSegment The segment index of the last segment up to (including) the end segment.
     * @return obsolete segments
     */
    @VisibleForTesting
    List<SegmentHandle> closeSegmentHandlers(long endSegment) {
        List<SegmentHandle> obsoleteSegments = channels.values()
                .stream()
                .filter(segmentHandle -> segmentHandle.getSegment() <= endSegment)
                .collect(Collectors.toList());

        for (SegmentHandle segment : obsoleteSegments) {
            close(segment);
        }

        return obsoleteSegments;
    }

    public void addSegmentToSync(SegmentHandle segment) {
        channelsToSync.add(segment.getWriteChannel());
    }

    /**
     * Close all segments
     */
    public void close() {
        List<SegmentHandle> currentChannels = new ArrayList<>(channels.values());

        for (SegmentHandle segment : currentChannels) {
            close(segment);
        }
    }

    public int getAmountOfChannelsToSync() {
        return channelsToSync.size();
    }

    public int getAmountOfOpenSegments() {
        return channels.size();
    }

    @FunctionalInterface
    interface AddressSpaceLoader {
        void load(SegmentHandle segment) throws IOException;
    }
}
