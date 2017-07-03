package org.corfudb.infrastructure.log;

import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * A SegmentHandle is a range view of consecutive addresses in the log. It contains
 * the address space along with metadata like addresses that are trimmed and pending trims.
 *
 * Created by maithem on 6/27/17.
 */

@Slf4j
@Data
class SegmentHandle {
    @Getter
    final long segment;

    @Getter
    @NonNull
    final FileChannel logChannel;

    @Getter
    @NonNull
    final FileChannel readChannel;

    @Getter
    @NonNull
    final FileChannel trimmedChannel;

    @Getter
    @NonNull
    final FileChannel pendingTrimChannel;

    @Getter
    @NonNull
    String fileName;

    @Getter
    @NonNull
    Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap();

    @Getter
    @NonNull
    Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Getter
    @NonNull
    Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public void close() {
        Set<FileChannel> channels =
                new HashSet(Arrays.asList(logChannel, readChannel, trimmedChannel, pendingTrimChannel));
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