package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request to replicate a file segment.
 */
@Data
@AllArgsConstructor
public class FileSegmentReplicationRequest implements ICorfuPayload<FileSegmentReplicationRequest> {

    private long fileSegmentIndex;
    private byte[] fileBuffer;
    private boolean inProgress;

    public FileSegmentReplicationRequest(ByteBuf buf) {
        fileSegmentIndex = ICorfuPayload.fromBuffer(buf, Long.class);
        fileBuffer = ICorfuPayload.fromBuffer(buf, byte[].class);
        inProgress = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, fileSegmentIndex);
        ICorfuPayload.serialize(buf, fileBuffer);
        ICorfuPayload.serialize(buf, inProgress);
    }
}
