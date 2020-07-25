package org.corfudb.protocols.wireprotocol.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This message represents a log entry to be replicated across remote sites. It is also used
 * as an ACK for a replicated entry, where the payload is empty.
 *
 * @author annym
 */
@Slf4j
@Data
public class LogReplicationEntry implements ICorfuPayload<LogReplicationEntry> {

    private LogReplicationEntryMetadata metadata;

    private List<OpaqueEntry> opaqueEntryList = new ArrayList<>();


    // Only used by test cases
    @VisibleForTesting
    private byte[] payload;

    @VisibleForTesting
    public LogReplicationEntry(LogReplicationEntryMetadata metadata, byte[] data) {
        this.metadata = metadata;
        this.payload = data;
    }

    public LogReplicationEntry(LogReplicationEntryMetadata metadata) {
        this.metadata = metadata;
    }

    public LogReplicationEntry(LogReplicationEntryMetadata metadata, List<OpaqueEntry> opaqueEntryList) {
        this.metadata = metadata;
        this.opaqueEntryList = opaqueEntryList;
    }

    public LogReplicationEntry(MessageType type, long epoch, UUID syncRequestId, long entryTS, long preTS, long snapshot, long sequence,
                               OpaqueEntry opaqueEntry) {
        this.metadata = new LogReplicationEntryMetadata(type, epoch, syncRequestId, entryTS, preTS, snapshot, sequence);
        this.opaqueEntryList.add(opaqueEntry);
    }

    public LogReplicationEntry(MessageType type, long epoch, UUID syncRequestId, long entryTS, long preTS, long snapshot, long sequence,
                               List<OpaqueEntry> opaqueEntryList) {
        this.metadata = new LogReplicationEntryMetadata(type, epoch, syncRequestId, entryTS, preTS, snapshot, sequence);
        this.opaqueEntryList.addAll(opaqueEntryList);
    }

    public LogReplicationEntry(ByteBuf buf) {
        metadata = ICorfuPayload.fromBuffer(buf, LogReplicationEntryMetadata.class);
        int opaqueEntryListSize = ICorfuPayload.fromBuffer(buf, Integer.class);
        for (int i = 0; i < opaqueEntryListSize; i++) {
            byte[] data = ICorfuPayload.fromBuffer(buf, byte[].class);
            opaqueEntryList.add(OpaqueEntry.deserialize(Unpooled.wrappedBuffer(data)));
        }

        log.trace("frombuf: opaqueEntryList {}", opaqueEntryList);
    }

    public static LogReplicationEntry generateAck(LogReplicationEntryMetadata metadata) {
        return new LogReplicationEntry(metadata);
    }

    public static LogReplicationEntry fromProto(Messages.LogReplicationEntry proto) {
        LogReplicationEntryMetadata metadata = LogReplicationEntryMetadata.fromProto(proto.getMetadata());
        ByteBuf dataBuf = Unpooled.copiedBuffer(proto.getData().toByteArray());

        int opaqueEntryListSize = ICorfuPayload.fromBuffer(dataBuf, Integer.class);

        ArrayList<OpaqueEntry> opaqueEntryList = new ArrayList<>();

        for (int i = 0; i < opaqueEntryListSize; i++) {
            opaqueEntryList.add(OpaqueEntry.deserialize(dataBuf));
        }

        log.trace("Msgtype {} fromProto: opaqueEntryList {}", metadata, opaqueEntryList) ;
        return new LogReplicationEntry(metadata, opaqueEntryList);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, metadata);

        Integer size = opaqueEntryList.size();
        ICorfuPayload.serialize(buf, size);

        for (OpaqueEntry opaqueEntry : opaqueEntryList) {
            ICorfuPayload.serialize(buf, opaqueEntry);
        }
    }

    public byte[] getPayload() {
        if (payload != null) {
            return payload;
        }
        ByteBuf buf = Unpooled.buffer();
        Integer size = opaqueEntryList.size();
        ICorfuPayload.serialize(buf, size);

        for (OpaqueEntry opaqueEntry : opaqueEntryList) {
            OpaqueEntry.serialize(buf, opaqueEntry);
        }
        payload =  buf.array();
        return payload;
    }
}
