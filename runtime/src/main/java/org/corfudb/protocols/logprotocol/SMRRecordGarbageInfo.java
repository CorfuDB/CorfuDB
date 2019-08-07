package org.corfudb.protocols.logprotocol;


import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ICorfuSerializable;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SMREntryGarbageInfo maintains garbage information of one SMREntry. This SMREntry could be stand-alone or a part of
 * MultiSMREntry. The existence of SMREntryGarbageInfo itself indicates the associated SMREntry has been identified
 * as garbage. SMREntryGarbageInfo could be used individually or composites more complicated SMRGarbageInfo.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class SMRRecordGarbageInfo {

    /**
     * Global address that marked the associated SMREntry as garbage.
     */
    @Getter
    private long markerAddress;

    /**
     * The size of the associated SMREntry in Byte.
     */
    @Getter
    private int smrEntrySize;

    /**
     * Constructor for SMREntryGarbageInfo
     * @param markerAddress
     * @param smrEntrySize
     */
    public SMRRecordGarbageInfo(long markerAddress, int smrEntrySize) {
        this.markerAddress = markerAddress;
        this.smrEntrySize = smrEntrySize;
    }

    /**
     * Get the serialized size of garbage-identified SMREntries that has marker address up to addressUpTo.
     * @param  addressUpTo Upper-bound marker address.
     * @return size in Byte.
     */
    public int getGarbageSizeUpTo(long addressUpTo) {
        return markerAddress <= addressUpTo ? smrEntrySize : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof SMRRecordGarbageInfo) {
            SMRRecordGarbageInfo otherGarbageInfo = (SMRRecordGarbageInfo) other;
            return otherGarbageInfo.markerAddress == markerAddress
                    && otherGarbageInfo.smrEntrySize == smrEntrySize;
        } else {
            return false;
        }
    }

    /**
     * Serialize the given SRMRecordGarbageInfo into a given byte buffer.
     *
     * @param b The buffer to serialize into.
     */
    public void serialize(ByteBuf b) {
        b.writeLong(markerAddress);
        b.writeInt(smrEntrySize);
    }

    /**
     * Deserialize SMRRecordGarbageInfo from a given byte buffer.
     * @param b  The buffer to deserialize.
     * @param rt corfu runtime.
     */
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        markerAddress = b.readLong();
        smrEntrySize = b.readInt();
    }
}
