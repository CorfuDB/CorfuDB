package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MultiSMREntryGarbageInfo maintains garbage information of one MultiSMREntry.
 *
 * Created by Xin at 06/05/2019.
 */
@ToString(callSuper = true)
public class MultiSMREntryGarbageInfo extends LogEntry implements ISMRGarbageInfo {

    /**
     * A map to maintain information about garbage-identified SMREntry inside MultiSMREntry. The key of the map is
     * the index of the SMREntry inside the original MultiSMREntry.
     */
    @Getter
    private final Map<Integer, SMREntryGarbageInfo> garbageMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public MultiSMREntryGarbageInfo() {
        super(LogEntryType.MULTISMR_GARBAGE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SMREntryGarbageInfo> getGarbageInfo(UUID streamId, int index) {
        // streamId is disregarded
        return Optional.ofNullable(garbageMap.get(index));
    }

    /**
     *
     */
    @Override
    public Map<Integer, SMREntryGarbageInfo> getAllGarbageInfo(UUID streamId) {
        // streamId is disregarded
        return garbageMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return garbageMap.isEmpty();
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
        return garbageMap.values().stream()
                .map(smr -> smr.getGarbageSizeUpTo(addressUpTo))
                .reduce(0, (a, b) -> a + b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(UUID streamId, int index) {
        garbageMap.remove(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ISMRGarbageInfo merge(ISMRGarbageInfo other) {
        if (other instanceof MultiSMREntryGarbageInfo) {
            MultiSMREntryGarbageInfo uniqueGarbageInfo = new MultiSMREntryGarbageInfo();
            ((MultiSMREntryGarbageInfo) other).getGarbageMap().forEach((index, smrEntryGarbageInfo) -> {
                if (!garbageMap.containsKey(index)) {
                    garbageMap.put(index, smrEntryGarbageInfo);
                    uniqueGarbageInfo.add(index, smrEntryGarbageInfo);
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
        // streamId is disregarded.
        add(index, smrEntryGarbageInfo);
    }

    void add(int index, SMREntryGarbageInfo smrEntryGarbageInfo) {
        garbageMap.put(index, smrEntryGarbageInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof MultiSMREntryGarbageInfo) {
            return garbageMap.equals(((MultiSMREntryGarbageInfo) other).getGarbageMap());
        } else {
            return false;
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(garbageMap.size());
        garbageMap.forEach((streamId, smrEntryGarbageInfo) -> {
            b.writeInt(streamId);
            Serializers.CORFU.serialize(smrEntryGarbageInfo, b);
        });
    }

    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int numEntries = b.readInt();
        garbageMap.clear();

        for (int i = 0; i < numEntries; i++) {
            garbageMap.put(b.readInt(), (SMREntryGarbageInfo) Serializers.CORFU.deserialize(b, rt));
        }
    }
}
