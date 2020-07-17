package org.corfudb.infrastructure.log;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
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
    private final long segment;

    @NonNull
    private final FileChannel writeChannel;

    @NonNull
    private final FileChannel readChannel;

    @NonNull
    private final Path fileName;

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
        Arrays.asList(writeChannel, readChannel).forEach(channel -> {
            try {
                channel.force(true);
            } catch (IOException e) {
                log.debug("Can't force updates in the channel: {}", e.getMessage());
            } finally {
                IOUtils.closeQuietly(channel);
            }
        });
    }
}