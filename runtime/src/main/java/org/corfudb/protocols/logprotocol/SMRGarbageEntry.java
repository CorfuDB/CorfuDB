package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SMRGarbageEntry maintains the positions and other information of garbage-identified {@link SMRRecord}s inside one
 * AddressSpaceView log data. The garbage information would be consumed by LogUnit servers to reclaim disk space and
 * other housekeeping tasks, e.g. tracking the lower bound of snapshot address for transactions due to the loss of
 * some versions. Each SMRGarbageEntry instance is wrapped in one {@link org.corfudb.protocols.wireprotocol.LogData}
 * instance whose address is the same as that of the associated log data having SMREntries.
 * <p>
 * Created by Xin Li at 06/03/19.
 */
public class SMRGarbageEntry extends LogEntry {

    /**
     * A map to maintain information about garbage-identified SMREntry inside MultiObjectSMREntry.
     * The key of the map is the stream id.
     */
    @Getter
    private final Map<UUID, Map<Integer, SMRGarbageRecord>> streamIdToGarbageMap = new ConcurrentHashMap<>();

    public SMRGarbageEntry() {
        super(LogEntryType.SMRGARBAGE);
    }

    /**
     * Get the garbage information about a specified SMREntry.
     *
     * @param streamId stream ID that the interested SMREntry is from.
     * @param index    per-stream index of the interested SMREntry.
     * @return if the SMREntry is identified as garbage, return the garbage information; otherwise return null.
     */
    @Nullable
    public SMRGarbageRecord getGarbageRecord(UUID streamId, int index) {
        if (!streamIdToGarbageMap.containsKey(streamId)) {
            return null;
        }
        return streamIdToGarbageMap.get(streamId).get(index);
    }

    /**
     * Get all the garbage information of a stream.
     *
     * @param streamId stream ID.
     * @return a map whose key is the per-stream index of garbage-identified SMREntries.
     */
    @Nullable
    public Map<Integer, SMRGarbageRecord> getGarbageRecords(UUID streamId) {
        return streamIdToGarbageMap.get(streamId);
    }

    /**
     * Get all the garbage information from all streams.
     *
     * @return a map from streamId to another map whose key is the per-stream index of garbage-identified SMREntries.
     */
    public Map<UUID, Map<Integer, SMRGarbageRecord>> getAllGarbageRecords() {
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
                .reduce(0, Integer::sum);
    }

    /**
     * Get the total number of SMRRecords wrapped in this SMRGarbageEntry instance.
     *
     * @return total number of SMRRecords.
     */
    public int getGarbageRecordCount() {
        return streamIdToGarbageMap.values()
                .stream()
                .map(Map::size)
                .reduce(0, Integer::sum);
    }

    /**
     * Remove information about one garbage-identified SMREntry.
     *
     * @param streamId stream ID the SMREntry belongs to.
     * @param index    per-stream index of the SMREntry in the associated log data.
     */
    public void remove(UUID streamId, int index) {
        Map<Integer, SMRGarbageRecord> recordMap = streamIdToGarbageMap.get(streamId);
        if (recordMap != null) {
            recordMap.remove(index);
            if (recordMap.isEmpty()) {
                streamIdToGarbageMap.remove(streamId);
            }
        }
    }

    /**
     * Prune the same garbage information contained in {@param pruneInfo}.
     *
     * @param pruneInfo pruning information
     */
    public void prune(@Nullable Map<UUID, List<Integer>> pruneInfo) {
        if (pruneInfo == null) {
            return;
        }

        if (pruneInfo == Collections.EMPTY_MAP) {
            streamIdToGarbageMap.clear();
            return;
        }

        pruneInfo.forEach((streamId, indexes) -> {
            if (indexes == Collections.EMPTY_LIST) {
                streamIdToGarbageMap.remove(streamId);
            } else {
                indexes.forEach(index -> remove(streamId, index));
            }
        });
    }

    /**
     * Merge garbage information from another SMRGarbageEntry instance.
     *
     * @param other another SMRGarbageEntry instance. It may be modified.
     * @return de-duplicated SMRGarbageEntry from parameter.
     */
    public SMRGarbageEntry merge(SMRGarbageEntry other) {
        List<UUID> streamToDelete = new ArrayList<>();
        other.getAllGarbageRecords().forEach((streamId, garbageMap) -> {
            if (garbageMap.isEmpty()) {
                streamToDelete.add(streamId);
            } else {
                Map<Integer, SMRGarbageRecord> gm = streamIdToGarbageMap.computeIfAbsent(
                        streamId, id -> new ConcurrentHashMap<>());
                garbageMap.keySet().removeAll(gm.keySet());
                // GarbageMap may become empty due to removeAll.
                if (garbageMap.isEmpty()) {
                    streamToDelete.add(streamId);
                } else {
                    streamIdToGarbageMap.get(streamId).putAll(garbageMap);
                }
            }
        });

        other.getAllGarbageRecords().keySet().removeAll(streamToDelete);
        return other;
    }

    /**
     * Deduplicate another SMRGarbageEntry instance based on the current instance.
     * Similar to {@link this#merge(SMRGarbageEntry)}, but does not change the current instance.
     *
     * @param other another SMRGarbageEntry instance. It may be modified.
     * @return de-duplicated SMRGarbageEntry from parameter.
     */
    public SMRGarbageEntry dedup(SMRGarbageEntry other) {
        List<UUID> streamToDelete = new ArrayList<>();
        other.getAllGarbageRecords().forEach((streamId, garbageMap) -> {
            if (garbageMap.isEmpty()) {
                streamToDelete.add(streamId);
            } else {
                Map<Integer, SMRGarbageRecord> gm = streamIdToGarbageMap.get(streamId);
                if (gm != null) {
                    garbageMap.keySet().removeAll(gm.keySet());
                }
                // GarbageMap may become empty due to removeAll.
                if (garbageMap.isEmpty()) {
                    streamToDelete.add(streamId);
                }
            }
        });

        other.getAllGarbageRecords().keySet().removeAll(streamToDelete);
        return other;
    }

    /**
     * Add information about one garbage-identified SMREntry.
     *
     * @param streamId          stream ID the SMREntry belongs to.
     * @param index             per-stream index of the SMREntry in the associated log data.
     * @param recordGarbageInfo garbage information about the SMREntry.
     */
    public void add(UUID streamId, int index, SMRGarbageRecord recordGarbageInfo) {
        streamIdToGarbageMap.computeIfAbsent(streamId, id -> new ConcurrentHashMap<>());
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
        if (other instanceof SMRGarbageEntry) {
            return streamIdToGarbageMap.equals(((SMRGarbageEntry) other).getStreamIdToGarbageMap());
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
            Map<Integer, SMRGarbageRecord> garbageInfoMap = new ConcurrentHashMap<>();
            int entryNum = b.readInt();
            for (int j = 0; j < entryNum; ++j) {
                int index = b.readInt();
                SMRGarbageRecord recordGarbageInfo = new SMRGarbageRecord();
                recordGarbageInfo.deserializeBuffer(b, rt);
                garbageInfoMap.put(index, recordGarbageInfo);
            }
            streamIdToGarbageMap.put(streamId, garbageInfoMap);
        }
    }
}
