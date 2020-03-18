package org.corfudb.infrastructure.log;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
class SegmentHandle {

    // Segment number.
    @EqualsAndHashCode.Include
    private final long segment;

    // Name of the underlying segment file.
    @NonNull
    @EqualsAndHashCode.Include
    private final String fileName;

    // File channel for log writes.
    @NonNull
    private final FileChannel writeChannel;

    // File channel for reads.
    @NonNull
    private final FileChannel readChannel;

    // Index of log entries that are guaranteed to be persisted.
    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    // Index of log entries that are written but not guaranteed to be persisted.
    private final Map<Long, AddressMetaData> pendingAddresses = new ConcurrentHashMap<>();

    // Reference count of this segment.
    private AtomicInteger refCount = new AtomicInteger(0);

    /**
     * Increase reference count.
     */
    public void retain() {
        refCount.incrementAndGet();
    }

    /**
     * Decrease reference count.
     */
    public void release() {
        refCount.updateAndGet(ref -> {
            if (ref <= 0) {
                throw new IllegalStateException("refCount cannot be less than 0, segment: " + segment);
            }
            return ref - 1;
        });
    }

    /**
     * Get the current reference count.
     */
    public int getRefCount() {
        return refCount.get();
    }

    /**
     * Check if the provided address is written to the segment, regardless
     * of whether persisted on disk.
     *
     * @param address the address to check if written
     * @return true if address is written to this segment, false otherwise
     */
    public boolean isAddressWritten(long address) {
        return knownAddresses.containsKey(address) || pendingAddresses.containsKey(address);
    }

    /**
     * Merge the pending addresses into the known addresses, making
     * pending addresses visible to the readers. This method should
     * be called after the addresses in the pending addresses are
     * guaranteed to be in secondary storage.
     */
    public void mergePendingAddresses() {
        knownAddresses.putAll(pendingAddresses);
        pendingAddresses.clear();
    }

    /**
     * Force the log writes in write channel to secondary storage.
     *
     * @throws IOException if sync failed
     */
    public void sync() throws IOException {
        writeChannel.force(true);
    }

    /**
     * Close the file channels and force updates to secondary storage.
     */
    public void close() {
        Set<FileChannel> channels = new HashSet<>(
                Arrays.asList(writeChannel, readChannel)
        );

        for (FileChannel channel : channels) {
            try {
                channel.force(true);
            } catch (IOException e) {
                log.debug("Can't force updates in the channel", e.getMessage());
            } finally {
                IOUtils.closeQuietly(channel);
            }
        }

        refCount.set(0);
    }
}