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
    final Map<UUID, Long> streamAddresses;
    @Getter
    final LogData data;

    @SuppressWarnings("unchecked")
    public WriteRequest(ByteBuf buf) {
        writeMode = ICorfuPayload.fromBuffer(buf, WriteMode.class);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            streamAddresses = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        } else { streamAddresses = null; }
        data = ICorfuPayload.fromBuffer(buf, LogData.class);
    }

    public WriteRequest(WriteMode writeMode, Map<UUID, Long> streamAddresses, ByteBuf buf) {
        this.writeMode = writeMode;
        this.streamAddresses = streamAddresses;
        this.data = new LogData(DataType.DATA, buf);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, writeMode);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            ICorfuPayload.serialize(buf, streamAddresses);
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
