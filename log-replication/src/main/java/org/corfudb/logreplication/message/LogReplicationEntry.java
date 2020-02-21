package org.corfudb.logreplication.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;

import java.util.UUID;

public class LogReplicationEntry {

    @Getter
    public LogReplicationEntryMetadata metadata;

    @Getter
    private byte[] payload;

    public LogReplicationEntry(LogReplicationEntryMetadata metadata, byte[] payload) {
        this.payload = payload;
        this.metadata = metadata;
    }

    public LogReplicationEntry(MessageType type, UUID syncRequestId, long entryTS, long preTS, long snapshot, long sequence,
                               byte[] payload) {
        this.metadata = new LogReplicationEntryMetadata(type, syncRequestId, entryTS, preTS, snapshot, sequence);
        this.payload = payload;
    }

    public byte[] serialize() {
        ByteBuf buf = Unpooled.buffer();

        // Metadata
        buf.writeInt(metadata.getMessageMetadataType().getVal());
        buf.writeLong(metadata.getPreviousTimestamp());
        buf.writeLong(metadata.getSyncRequestId().getMostSignificantBits());
        buf.writeLong(metadata.getSyncRequestId().getLeastSignificantBits());
        buf.writeLong(metadata.getSnapshotSyncSeqNum());
        buf.writeLong(metadata.getSnapshotTimestamp());
        buf.writeLong(metadata.getTimestamp());

        // Data
        buf.writeInt(payload.length);
        buf.writeBytes(payload);

        return buf.array();
    }

    public static LogReplicationEntry deserialize(byte[] data) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(data);

        // Deserialize Metadata
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata();
        metadata.setMessageMetadataType(MessageType.fromValue(byteBuf.readInt()));
        metadata.setPreviousTimestamp(byteBuf.readLong());
        UUID uuid = new UUID(byteBuf.readLong(), byteBuf.readLong());
        metadata.setSyncRequestId(uuid);
        metadata.setSnapshotSyncSeqNum(byteBuf.readLong());
        metadata.setSnapshotTimestamp(byteBuf.readLong());
        metadata.setTimestamp(byteBuf.readLong());

        // Deserialize Data
        int size = byteBuf.readInt();
        ByteBuf dataBuf = Unpooled.buffer(size);
        byteBuf.readBytes(dataBuf, size);
        byte[] payload = dataBuf.array();

        return new LogReplicationEntry(metadata, payload);
    }
}
