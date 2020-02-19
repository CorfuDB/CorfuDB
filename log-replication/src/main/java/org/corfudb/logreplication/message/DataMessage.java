package org.corfudb.logreplication.message;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.logprotocol.OpaqueEntry;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This class represents the data message, it contains the data to be replicated and metadata required for sequencing.
 */
@Data
public class DataMessage {

    @Getter
    public MessageMetadata metadata;

    @Setter
    private byte[] data;

    public DataMessage() {}

    @VisibleForTesting
    public DataMessage(byte[] data) {
        this.data = data;
    }

    public DataMessage(MessageMetadata metadata) {
        this.metadata = metadata;
    }

    public DataMessage(MessageType type, long entryTS, long preTS, long snapshot, long sequence, byte[] data) {
        metadata = new MessageMetadata(type, entryTS, preTS, snapshot, sequence);
        this.data = data;
    }

    public DataMessage(MessageType type, long entryTS, long preTS, long snapshot, long sequence,
                       OpaqueEntry opaqueEntry) {
        metadata = new MessageMetadata(type, entryTS, preTS, snapshot, sequence);
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(metadata.getMessageMetadataType().getVal());
        buf.writeLong(metadata.getPreviousTimestamp());
        buf.writeLong(metadata.getSnapshotRequestId().getMostSignificantBits());
        buf.writeLong(metadata.getSnapshotRequestId().getLeastSignificantBits());
        buf.writeLong(metadata.getSnapshotSyncSeqNum());
        buf.writeLong(metadata.getSnapshotTimestamp());
        buf.writeLong(metadata.getTimestamp());
        OpaqueEntry.serialize(buf, opaqueEntry);
        this.data = buf.array();
    }

    public void setData(byte[] data) {

        MessageMetadata metadata = new MessageMetadata();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);


        metadata.setMessageMetadataType(MessageType.fromValue(byteBuf.readInt()));
        metadata.setPreviousTimestamp(byteBuf.readLong());

        UUID uuid = new UUID(byteBuf.readLong(), byteBuf.readLong());
        metadata.setSnapshotRequestId(uuid);

        metadata.setSnapshotSyncSeqNum(byteBuf.readLong());
        metadata.setSnapshotTimestamp(byteBuf.readLong());
        metadata.setTimestamp(byteBuf.readLong());

        this.metadata = metadata;
        this.data = byteBuf.array();
    }
}
