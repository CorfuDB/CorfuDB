package org.corfudb.infrastructure.log;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The global log is partition into segments, each segmentNumber contains a range of consecutive
 * addresses. Accessing the address space for a particular segmentNumber happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@Data
class SegmentHandle {
    final long segmentNumber;

    //TODO(Maithem): Hide both write/read channels
    @NonNull
    final FileChannel writeChannel;

    @NonNull
    final FileChannel readChannel;

    @NonNull
    String fileName;

    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();
    private final Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private AtomicLong refCount = new AtomicLong();

    public void flush() throws IOException {
        writeChannel.force(true);
    }

    public void retain() {
        refCount.incrementAndGet();
    }

    public void release() {
        refCount.updateAndGet(currVal -> {
            if (currVal == 0) {
                throw new IllegalStateException("refCount cannot be less than 0, segmentNumber " + segmentNumber);
            }
            return currVal - 1;
        });
    }

    public void close() {
        Set<FileChannel> channels = new HashSet<>(
                Arrays.asList(writeChannel, readChannel)
        );

        // TODO(Maithem): Print a warning if the refcount != 0
        for (FileChannel channel : channels) {
            try {
                channel.force(true);
            } catch (IOException e) {
                log.debug("Can't force updates in the channel", e.getMessage());
            } finally {
                IOUtils.closeQuietly(channel);
            }
        }
    }
}