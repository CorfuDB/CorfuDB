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
    final EnumMap<IMetadata.LogUnitMetadataType, Object> metadataMap =
            new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    @Getter
    final ByteBuf dataBuffer;

    @SuppressWarnings("unchecked")
    public WriteRequest(ByteBuf buf) {
        writeMode = ICorfuPayload.fromBuffer(buf, WriteMode.class);
        globalAddress = ICorfuPayload.fromBuffer(buf, Long.class);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            streamID = ICorfuPayload.fromBuffer(buf, UUID.class);
        } else { streamID = null; }
        metadataMap.putAll(ICorfuPayload.enumMapFromBuffer(buf, IMetadata.LogUnitMetadataType.class, Object.class));
        dataBuffer = ICorfuPayload.fromBuffer(buf, ByteBuf.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, writeMode);
        ICorfuPayload.serialize(buf, globalAddress);
        if (writeMode == WriteMode.REPLEX_STREAM) {
            ICorfuPayload.serialize(buf, streamID);
        }
        ICorfuPayload.serialize(buf, metadataMap);
        ICorfuPayload.serialize(buf, dataBuffer);
    }

    // This class sets defaults for the builder pattern.
    public static class WriteRequestBuilder {
        private WriteMode writeMode = WriteMode.NORMAL;
    }
}
