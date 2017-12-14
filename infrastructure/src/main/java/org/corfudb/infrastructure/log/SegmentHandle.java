package org.corfudb.infrastructure.log;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

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
    final FileChannel trimmedChannel;

    @NonNull
    final FileChannel pendingTrimChannel;

    @NonNull
    String fileName;

    private Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap();
    private Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());
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
        Set<FileChannel> channels =
                new HashSet(Arrays.asList(writeChannel, readChannel, trimmedChannel, pendingTrimChannel));
        for (FileChannel channel : channels) {
            try {
                channel.force(true);
                channel.close();
                channel = null;
            } catch (Exception e) {
                log.warn("Error closing channel {}: {}", channel.toString(), e.toString());
            }
        }

        knownAddresses = null;
        trimmedAddresses = null;
        pendingTrims = null;
    }
}