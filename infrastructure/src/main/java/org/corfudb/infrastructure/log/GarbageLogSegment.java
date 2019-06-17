package org.corfudb.infrastructure.log;

import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.protocols.wireprotocol.LogData;

/**
 * Garbage log segment, has one-to-one mapping to a stream log segment.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
public class GarbageLogSegment extends AbstractLogSegment {

    public GarbageLogSegment(long startAddress, String fileName,
                             StreamLogParams logParams,
                             ResourceQuota logSizeQuota,
                             SegmentMetaData segmentMetaData) {
        super(startAddress, fileName, logParams, logSizeQuota, segmentMetaData);
    }

    public void append(long address, LogData entry) {

    }

    @Override
    public void close() {

    }
}
