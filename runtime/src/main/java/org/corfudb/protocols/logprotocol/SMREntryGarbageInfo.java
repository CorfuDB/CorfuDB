package org.corfudb.protocols.logprotocol;


import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SMREntryGarbageInfo maintains garbage information of one SMREntry. This SMREntry could be stand-alone or a part of
 * MultiSMREntry. The existence of SMREntryGarbageInfo itself indicates the associated SMREntry has been identified
 * as garbage. SMREntryGarbageInfo could be used individually or composites more complicated ISMRGarbageInfo.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class SMREntryGarbageInfo extends LogEntry implements ISMRGarbageInfo {

    /**
     * Global address that detected the associated SMREntry as garbage.
     */
    @Getter
    private long detectorAddress;

    /**
     * The size of the associated SMREntry in Byte.
     */
    @Getter
    private int smrEntrySize;

    /**
     * Constructor for SMREntryGarbageInfo
     * @param detectorAddress
     * @param smrEntrySize
     */
    public SMREntryGarbageInfo(long detectorAddress, int smrEntrySize) {
        super(LogEntryType.SMR_GARBAGE);
        this.detectorAddress = detectorAddress;
        this.smrEntrySize = smrEntrySize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SMREntryGarbageInfo> getGarbageInfo(UUID streamId, int index) {
        // both streamId and index are not checked
        return Optional.of(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, SMREntryGarbageInfo> getAllGarbageInfo(UUID streamId) {
        // streamId is disregarded
        Map<Integer, SMREntryGarbageInfo> garbageInfo = new ConcurrentHashMap<>();
        garbageInfo.put(0, this);
        return garbageInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getGarbageSize() {
        return smrEntrySize;
    }

    /**
     * {@inheritDoc}
     */
    public int getGarbageSizeUpTo(long addressUpTo) {
        return detectorAddress < addressUpTo ? smrEntrySize : 0;
    }

    /**
     * {@inheritDoc}
     *
     * SMREntryGarbageInfo does not support this operation.
     */
    @Override
    public void remove(UUID streamId, int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * SMREntryGarbageInfo does not support this operation.
     */
    @Override
    public ISMRGarbageInfo merge(ISMRGarbageInfo other) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * SMREntryGarbageInfo does not support this operation.
     */
    @Override
    public void add(UUID streamId, int index, SMREntryGarbageInfo smrEntryGarbageInfo) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof SMREntryGarbageInfo) {
            return ((SMREntryGarbageInfo) other).detectorAddress == detectorAddress
                    && ((SMREntryGarbageInfo) other).getGarbageSize() == smrEntrySize;
        } else {
            return false;
        }
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeLong(detectorAddress);
        b.writeInt(smrEntrySize);
    }

    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        detectorAddress = b.readLong();
        smrEntrySize = b.readInt();
    }

}
