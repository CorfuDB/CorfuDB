package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.apache.commons.collections.map.HashedMap;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SMRGarbageInfo maintains the positions and other information of garbage-identified {@link SMRRecord}s inside one
 * AddressSpaceView log data. The garbage information would be consumed by LogUnit servers to reclaim disk space and
 * other housekeeping tasks, e.g. tracking the lower bound of snapshot address for transactions due to the loss of
 * some versions. Each SMRGarbageInfo instance is wrapped in one {@link org.corfudb.protocols.wireprotocol.LogData}
 * instance whose address is the same as that of the associated log data having SMREntries.
 *
 * Created by Xin Li at 06/03/19.
 */
public class SMRGarbageInfo extends LogEntry {

    /**
     * A map to maintain information about garbage-identified SMREntry inside MultiObjectSMREntry.
     * The key of the map is the stream id.
     */
    @Getter
    private final Map<UUID, Map<Integer, SMRRecordGarbageInfo>> streamIdToGarbageMap = new ConcurrentHashMap<>();

    private static final Map<Integer, SMRRecordGarbageInfo> EMPTY_GARBAGE_MAP = new ConcurrentHashMap<>();

    public SMRGarbageInfo() {
        super(LogEntryType.SMRGARBAGE);
    }

    /**
     * Get the garbage information about a specified SMREntry.
     *
     * @param streamId stream ID that the interested SMREntry is from.
     * @param index    per-stream index of the interested SMREntry.
     * @return if the SMREntry is identified as garbage, return the garbage information; otherwise return null.
     */
    public Optional<SMRRecordGarbageInfo> getGarbageInfo(UUID streamId, int index) {
        if (!streamIdToGarbageMap.containsKey(streamId)) {
            return Optional.empty();
        }

        return Optional.ofNullable(streamIdToGarbageMap.get(streamId).get(index));
    }

    /**
     * Get all the garbage information of a stream.
     *
     * @param streamId stream ID.
     * @return a map whose key is the per-stream index of garbage-identified SMREntries.
     */
    public Map<Integer, SMRRecordGarbageInfo> getAllGarbageInfo(UUID streamId) {
        if (streamIdToGarbageMap.containsKey(streamId)) {
            return streamIdToGarbageMap.get(streamId);
        } else {
            return EMPTY_GARBAGE_MAP;
        }
    }

    /**
     * Get all the garbage information from all streams.
     *
     * @return a map from streamId to another map whose key is the per-stream index of garbage-identified SMREntries.
     */
    public Map<UUID, Map<Integer, SMRRecordGarbageInfo>> getAllGarbageInfo() {
        return streamIdToGarbageMap;
    }

    /**
     * Check if there is no garbage information.
     *
     * @return true if there is no garbage information, false otherwise.
     */
    public boolean isEmpty() {
        return streamIdToGarbageMap.isEmpty();
    }

    /**
     * Get the total serialized size of all garbage-identified SMREntries in the associated log data.
     *
     * @return size in Byte.
     */
    public int getGarbageSize() {
        return getGarbageSizeUpTo(Long.MAX_VALUE);
    }

    /**
     * Get the serialized size of garbage-identified SMREntries that has marker address
     * up to addressUpTo.
     *
     * @param addressUpTo marker address up to which garbage sizes are counted
     * @return size in Byte.
     */
    public int getGarbageSizeUpTo(long addressUpTo) {
        return streamIdToGarbageMap.values().stream()
                .flatMap(multiSMRGarbage -> multiSMRGarbage.values().stream())
                .map(smrRecordGarbage -> smrRecordGarbage.getGarbageSizeUpTo(addressUpTo))
                .reduce(0, (a, b) -> a + b);
    }

    /**
     * Remove information about one garbage-identified SMREntry.
     *
     * @param streamId stream ID the SMREntry belongs to.
     * @param index    per-stream index of the SMREntry in the associated log data.
     */
    public void remove(UUID streamId, int index) {
        if (streamIdToGarbageMap.containsKey(streamId)) {
            streamIdToGarbageMap.get(streamId).remove(streamId, index);
        }
    }

    /**
     * Merge garbage information from another SMRGarbageInfo instance.
     *
     * @param other another SMRGarbageInfo instance.
     * @return de-duplicated garbage information.
     */
    public void merge(SMRGarbageInfo other) {
        other.getAllGarbageInfo().forEach((streamId, garbageMap) -> {
            if (garbageMap.size() > 0) {
                if (!streamIdToGarbageMap.containsKey(streamId)) {
                    streamIdToGarbageMap.put(streamId, new HashMap<>());
                }
                streamIdToGarbageMap.get(streamId).putAll(garbageMap);
            }
        });
    }

    /**
     * Add information about one garbage-identified SMREntry.
     *
     * @param streamId            stream ID the SMREntry belongs to.
     * @param index               per-stream index of the SMREntry in the associated log data.
     * @param recordGarbageInfo   garbage information about the SMREntry.
     */
    public void add(UUID streamId, int index, SMRRecordGarbageInfo recordGarbageInfo) {
        if (!streamIdToGarbageMap.containsKey(streamId)) {
            streamIdToGarbageMap.put(streamId, new HashMap<>());
        }

        streamIdToGarbageMap.get(streamId).put(index, recordGarbageInfo);
    }

    /**
     * Test if the other object instance equals this instance.
     *
     * @param other another object.
     * @return true if equals
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof SMRGarbageInfo) {
            return streamIdToGarbageMap.equals(((SMRGarbageInfo) other).getStreamIdToGarbageMap());
        } else {
            return false;
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(streamIdToGarbageMap.size());
        streamIdToGarbageMap.forEach((streamId, garbageMap) -> {
            b.writeLong(streamId.getMostSignificantBits());
            b.writeLong(streamId.getLeastSignificantBits());
            b.writeInt(garbageMap.size());
            garbageMap.forEach((index, smrRecordGarbage) -> {
                b.writeInt(index);
                smrRecordGarbage.serialize(b);
            });
        });
    }

    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int streamNum = b.readInt();
        streamIdToGarbageMap.clear();

        for (int i = 0; i < streamNum; i++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());
            Map<Integer, SMRRecordGarbageInfo> garbageInfoMap = new HashMap<>();
            int entryNum = b.readInt();
            for (int j = 0; j < entryNum; ++j) {
                int index = b.readInt();
                SMRRecordGarbageInfo recordGarbageInfo = new SMRRecordGarbageInfo();
                recordGarbageInfo.deserializeBuffer(b, rt);
                garbageInfoMap.put(index, recordGarbageInfo);
            }
            streamIdToGarbageMap.put(streamId, garbageInfoMap);
        }
    }
}
