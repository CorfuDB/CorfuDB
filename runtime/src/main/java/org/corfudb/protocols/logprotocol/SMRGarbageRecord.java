package org.corfudb.protocols.logprotocol;


import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;

@ToString(callSuper = true)
@NoArgsConstructor
public class SMRGarbageRecord {

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
     * Constructor for SMRGarbageRecord
     * @param markerAddress The Global address of the SRMRecord that supersedes this SMRRecord.
     * @param smrRecordSize  Serialized size of this SMRRecord in byte.
     */
    public SMRGarbageRecord(long markerAddress, int smrRecordSize) {
        this.markerAddress = markerAddress;
        this.smrEntrySize = smrRecordSize;
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
        if (other instanceof SMRGarbageRecord) {
            SMRGarbageRecord otherGarbageInfo = (SMRGarbageRecord) other;
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
     * Deserialize SMRGarbageRecord from a given byte buffer.
     * @param b  The buffer to deserialize.
     * @param rt corfu runtime.
     */
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        markerAddress = b.readLong();
        smrEntrySize = b.readInt();
    }
}
