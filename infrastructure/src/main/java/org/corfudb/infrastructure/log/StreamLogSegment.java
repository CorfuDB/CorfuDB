package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.protocols.wireprotocol.LogData;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
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
@Getter
class StreamLogSegment extends AbstractLogSegment {

    private long garbagePayloadSize;

    private long totalPayloadSize;

    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    public StreamLogSegment(long segmentAddress, String fileName,
                            FileChannel writeChannel, FileChannel readChannel) {
        super(segmentAddress, fileName, writeChannel, readChannel);
    }

    public void recordSegmentStats(LogData logData) {
        if (logData.getData() == null) {
            throw new IllegalStateException("LogData is missing data field.");
        }
        totalPayloadSize += logData.getData().length;
    }

    public double getGarbagePayloadSizeMB() {
        return (double) garbagePayloadSize / 1024 / 1024;
    }

    public double getGarbageRatio() {
        return (double) garbagePayloadSize / totalPayloadSize;
    }

    @Override
    public void append(long address, LogData entry) {

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