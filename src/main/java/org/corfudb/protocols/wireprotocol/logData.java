package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.infrastructure.log.LogUnitEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ICorfuSerializable;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mwei on 8/15/16.
 */
public class LogData implements ICorfuPayload<LogData>, IMetadata, ILogData {

    public static final LogData EMPTY = new LogData(DataType.EMPTY);
    public static final LogData HOLE = new LogData(DataType.HOLE);

    @Getter
    final DataType type;

    @Getter
    ByteBuf data;

    private transient final AtomicReference<Object> payload = new AtomicReference<>();

    public Object getPayload(CorfuRuntime runtime) {
        Object value = payload.get();
        if (value == null) {
            synchronized (this.payload) {
                value = this.payload.get();
                if (value == null) {
                    if (data == null) {
                        this.payload.set(null);
                    }
                    else {
                        data.resetReaderIndex();
                        final Object actualValue = Serializers.getSerializer(Serializers.SerializerType.CORFU)
                                .deserialize(data, runtime);
                        // TODO: Possibly fix some dependencies here.
                        if (actualValue instanceof LogEntry) {
                            ((LogEntry) actualValue).setEntry(this);
                            ((LogEntry) actualValue).setRuntime(runtime);
                        }
                        value = actualValue == null ? this.payload : actualValue;
                        this.payload.set(value);
                    }
                }
            }
        }
        return value;
    }

    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap =
            new EnumMap<>(IMetadata.LogUnitMetadataType.class);

    public LogData(ByteBuf buf) {
        type = ICorfuPayload.fromBuffer(buf, DataType.class);
        if (type == DataType.DATA) {
            data = ICorfuPayload.fromBuffer(buf, ByteBuf.class);
            metadataMap.putAll(
                    ICorfuPayload.enumMapFromBuffer(buf,
                            IMetadata.LogUnitMetadataType.class, Object.class));
        }
    }

    public LogData(DataType type) {
        this.type = type;
    }

    public LogData(DataType type, ByteBuf buf) {
        this.type = type;
        this.data = buf;
    }

    public LogData(ByteBuf buf, EnumMap<LogUnitMetadataType, Object> metadataMap) {
        this.type = DataType.DATA;
        this.data = buf;
        this.metadataMap.putAll(metadataMap);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, type);
        if (type == DataType.DATA) {
            ICorfuPayload.serialize(buf, data);
            ICorfuPayload.serialize(buf, metadataMap);
        }
    }
}
