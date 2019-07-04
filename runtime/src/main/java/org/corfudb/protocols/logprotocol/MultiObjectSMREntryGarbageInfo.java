package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MultiObjectSMREntryGarbageInfo maintains garbage information of one MultiObjectSMREntry.
 *
 * Created by Xin at 06/05/2019.
 */
public class MultiObjectSMREntryGarbageInfo extends LogEntry implements ISMRGarbageInfo {

    /**
     * A map to maintain information about garbage-identified SMREntry inside MultiObjectSMREntry.
     * The key of the map is the stream id.
     */
    @Getter
    private final Map<UUID, MultiSMREntryGarbageInfo> streamIdToGarbageMap = new ConcurrentHashMap<>();

    public MultiObjectSMREntryGarbageInfo() {
        super(LogEntryType.MULTIOBJSMR_GARBAGE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SMREntryGarbageInfo> getGarbageInfo(UUID streamId, int index) {
        if (!streamIdToGarbageMap.containsKey(streamId)) {
            return Optional.empty();
        }

        return Optional.ofNullable(streamIdToGarbageMap.get(streamId).getGarbageMap().get(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, SMREntryGarbageInfo> getAllGarbageInfo(UUID streamId) {
        if (streamIdToGarbageMap.containsKey(streamId)) {
            return streamIdToGarbageMap.get(streamId).getAllGarbageInfo(streamId);
        } else {
            return ISMRGarbageInfo.EMPTY;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return streamIdToGarbageMap.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getGarbageSize() {
        return getGarbageSizeUpTo(Long.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    public int getGarbageSizeUpTo(long addressUpTo) {
        return streamIdToGarbageMap.values().stream()
                .map(multiSmr -> multiSmr.getGarbageSizeUpTo(addressUpTo))
                .reduce(0, (a, b) -> a + b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(UUID streamId, int index) {
        if (streamIdToGarbageMap.containsKey(streamId)) {
            streamIdToGarbageMap.get(streamId).remove(streamId, index);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ISMRGarbageInfo merge(ISMRGarbageInfo other) {
        if (other instanceof MultiObjectSMREntryGarbageInfo) {
            MultiObjectSMREntryGarbageInfo uniqueGarbageInfo = new MultiObjectSMREntryGarbageInfo();

            ((MultiObjectSMREntryGarbageInfo) other).getStreamIdToGarbageMap().forEach((streamId, garbageMap) -> {
                // disregard empty gc info
                if (garbageMap.getAllGarbageInfo(streamId).size() > 0) {
                    if (streamIdToGarbageMap.containsKey(streamId)) {
                        MultiSMREntryGarbageInfo perStreamDeUniqueGCInfo =
                                (MultiSMREntryGarbageInfo) streamIdToGarbageMap.get(streamId).merge(garbageMap);
                        // disregard if on new gc info is merged
                        if (!perStreamDeUniqueGCInfo.isEmpty()) {
                            uniqueGarbageInfo.add(streamId, perStreamDeUniqueGCInfo);
                        }
                    } else {
                        uniqueGarbageInfo.add(streamId, garbageMap);
                        this.add(streamId, garbageMap);
                    }
                }
            });

            return uniqueGarbageInfo;
        } else {
            throw new IllegalArgumentException("Different types of ISMRGarbageInfo cannot merge.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(UUID streamId, int index, SMREntryGarbageInfo smrEntryGarbageInfo) {
        if (!streamIdToGarbageMap.containsKey(streamId)) {
            streamIdToGarbageMap.put(streamId, new MultiSMREntryGarbageInfo());
        }

        streamIdToGarbageMap.get(streamId).add(index, smrEntryGarbageInfo);
    }

    void add(UUID streamId, MultiSMREntryGarbageInfo multiSMREntryGarbageInfo) {
        streamIdToGarbageMap.put(streamId, multiSMREntryGarbageInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof MultiObjectSMREntryGarbageInfo) {
            return streamIdToGarbageMap.equals(((MultiObjectSMREntryGarbageInfo) other).getStreamIdToGarbageMap());
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
            Serializers.CORFU.serialize(garbageMap, b);
        });
    }

    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int numEntries = b.readInt();
        streamIdToGarbageMap.clear();

        for (int i = 0; i < numEntries; i++) {
            streamIdToGarbageMap.put(
                    new UUID(b.readLong(), b.readLong()),
                    ((MultiSMREntryGarbageInfo) Serializers.CORFU.deserialize(b, rt))
            );
        }
    }
}
