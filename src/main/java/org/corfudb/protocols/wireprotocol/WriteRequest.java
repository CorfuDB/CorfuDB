package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;

import java.util.*;

/**
 * Created by mwei on 8/9/16.
 */
@Builder
@AllArgsConstructor
public class WriteRequest implements ICorfuPayload<WriteRequest>, IMetadata {

    @Getter
    final WriteMode writeMode;
    @Getter
    final Long globalAddress;
    @Getter
    final UUID streamID;
    @Getter
    final LogData data;

    @SuppressWarnings("unchecked")
    public WriteRequest(ByteBuf buf) {
        writeMode = ICorfuPayload.fromBuffer(buf, WriteMode.class);
        globalAddress = ICorfuPayload.fromBuffer(buf, Long.class);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            streamID = ICorfuPayload.fromBuffer(buf, UUID.class);
        } else { streamID = null; }
        data = ICorfuPayload.fromBuffer(buf, LogData.class);
    }

    public WriteRequest(WriteMode writeMode, Long globalAddress, UUID streamID, ByteBuf buf) {
        this.writeMode = writeMode;
        this.globalAddress = globalAddress;
        this.streamID = streamID;
        this.data = new LogData(DataType.DATA, buf);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, writeMode);
        ICorfuPayload.serialize(buf, globalAddress);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            ICorfuPayload.serialize(buf, streamID);
        }
        ICorfuPayload.serialize(buf, data);
    }

    @Override
    public EnumMap<LogUnitMetadataType, Object> getMetadataMap() {
        return data.getMetadataMap();
    }

    // This class sets defaults for the builder pattern.
    public static class WriteRequestBuilder {
        private WriteMode writeMode = WriteMode.NORMAL;
    }
}
