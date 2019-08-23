package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.Getter;
import lombok.Setter;
import lombok.NonNull;

import org.corfudb.util.serializer.ICorfuSerializable;

import java.util.UUID;


@ToString
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Getter
@Setter
public class SMRRecordLocator implements Comparable<SMRRecordLocator>, ICorfuSerializable {
    private long globalAddress;
    private UUID streamId;
    private int index;
    private int serializedSize;

    public void serialize(ByteBuf buf) {
        buf.writeLong(globalAddress);
        buf.writeLong(streamId.getMostSignificantBits());
        buf.writeLong(streamId.getLeastSignificantBits());
        buf.writeInt(index);
        buf.writeInt(serializedSize);
    }


    static public SMRRecordLocator deserialize(ByteBuf buf) {
        SMRRecordLocator smrRecordLocator = new SMRRecordLocator();

        smrRecordLocator.setGlobalAddress(buf.readLong());
        long mostSignificantBits = buf.readLong();
        long leastSignificantBits = buf.readLong();
        smrRecordLocator.setStreamId(new UUID(mostSignificantBits, leastSignificantBits));
        smrRecordLocator.setIndex(buf.readInt());
        smrRecordLocator.setSerializedSize(buf.readInt());

        return smrRecordLocator;
    }

    public int compareTo(@NonNull SMRRecordLocator other) {
        if (globalAddress != other.globalAddress) {
            return Long.compare(globalAddress, other.getGlobalAddress());
        }
        if (!streamId.equals(other.getStreamId())) {
            throw new IllegalArgumentException("Can't compare SMRRecordLocator of different streams");
        }

        return Integer.compare(index, other.getIndex());
    }
}
