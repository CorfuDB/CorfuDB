package org.corfudb.infrastructure.log;

import org.corfudb.protocols.wireprotocol.LogData;

import java.nio.channels.FileChannel;

/**
 * Garbage log segment, has one-to-one mapping to a stream log segment.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
public class GarbageLogSegment extends AbstractLogSegment {

    public GarbageLogSegment(long startAddress, String fileName,
                             FileChannel writeChannel, FileChannel readChannel) {
        super(startAddress, fileName, writeChannel, readChannel);
    }

    @Override
    public void append(long address, LogData entry) {

    }

    @Override
    public void close() {

    }
}
