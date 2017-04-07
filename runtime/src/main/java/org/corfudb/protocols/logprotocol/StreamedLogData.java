package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Temporary class for transition to log data.
 *
 * Created by mwei on 4/6/17.
 */
public class StreamedLogData implements ILogData {

    @Getter
    public final @Nonnull
    Map<UUID, StreamData> entryMap;

    public StreamedLogData(Map<UUID, StreamData> entryMap) {
        this.entryMap = entryMap;
    }

    public void getSerializedForm(BiConsumer<Object, ByteBuf> serializer, ByteBuf b) {
        // go through each stream data and serialize it.
    }

    @Override
    public Object getPayload(CorfuRuntime t) {
        return null;
    }

    @Override
    public DataType getType() {
        return null;
    }

    @Override
    public EnumMap<LogUnitMetadataType, Object> getMetadataMap() {
        return null;
    }
}
