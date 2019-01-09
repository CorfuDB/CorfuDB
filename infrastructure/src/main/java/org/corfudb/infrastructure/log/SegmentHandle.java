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

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@Data
class SegmentHandle {
    final long segment;

    @NonNull
    final FileChannel writeChannel;

    @NonNull
    final FileChannel readChannel;

    @NonNull
    String fileName;

    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();
    private final Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile int refCount = 0;


    public synchronized void retain() {
        refCount++;
    }

    public synchronized void release() {
        if (refCount == 0) {
            throw new IllegalStateException("refCount cannot be less than 0, segment " + segment);
        }
        refCount--;
    }

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
    }
}