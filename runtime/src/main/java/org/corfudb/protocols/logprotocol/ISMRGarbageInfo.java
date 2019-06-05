package org.corfudb.protocols.logprotocol;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ISMRGarbageInfo maintains the positions and other information of garbage-identified {@link SMREntry}s inside one
 * AddressSpaceView log data. The garbage information would be consumed by LogUnit servers to reclaim disk space and
 * other housekeeping tasks, e.g. tracking the lower bound of snapshot address for transactions due to the loss of
 * some versions. Each ISMRGarbageInfo instance is wrapped in one {@link org.corfudb.protocols.wireprotocol.LogData}
 * instance whose address is the same as that of the associated log data having SMREntries.
 *
 * <p>One log data contains one or multiple SMREntries, depending on the type of {@link ISMRConsumable} instance
 * wrapped in the log data. Each concrete implementation of ISMRGarbageInfo corresponds to one concrete
 * implementation of ISMRConsumable. <p/>
 *
 * Created by Xin Li at 06/03/19.
 */
public interface ISMRGarbageInfo {
    Map<Integer, SMREntryGarbageInfo> EMPTY = new ConcurrentHashMap<>();

    /**
     * Get the garbage information about a specified SMREntry.
     * @param streamId stream ID that the interested SMREntry is from.
     * @param index    per-stream index of the interested SMREntry.
     * @return         if the SMREntry is identified as garbage, return the garbage information; otherwise return null.
     */
    Optional<SMREntryGarbageInfo> getGarbageInfo(UUID streamId, int index);

    /**
     * Get all the garbage information of a stream.
     * @param streamId stream ID.
     * @return         a map whose key is the per-stream index of garbage-identified SMREntries.
     */
    Map<Integer, SMREntryGarbageInfo> getAllGarbageInfo(UUID streamId);

    /**
     * Get the total serialized size of all garbage-identified SMREntries in the associated log data.
     * @return size in Byte.
     */
    int getGarbageSize();

    /**
     * Add information about one garbage-identified SMREntry.
     * @param streamId             stream ID the SMREntry belongs to.
     * @param index                per-stream index of the SMREntry in the associated log data.
     * @param smrEntryGarbageInfo  garbage information about the SMREntry.
     */
    void add(UUID streamId, int index, SMREntryGarbageInfo smrEntryGarbageInfo);

    /**
     * Remove information about one garbage-identified SMREntry.
     * @param streamId stream ID the SMREntry belongs to.
     * @param index    per-stream index of the SMREntry in the associated log data.
     */
    void remove(UUID streamId, int index);

    /**
     * Merge garbage information from another ISMRGarbageInfo instance.
     * @param other another ISMRGarbageInfo instance.
     * @return deduplicated garbage information.
     */
    ISMRGarbageInfo merge(ISMRGarbageInfo other);

    /**
     * Test if the other object instance equals this instance.
     * @param other another object.
     * @return true if equals
     */
    boolean equals(Object other);
}
