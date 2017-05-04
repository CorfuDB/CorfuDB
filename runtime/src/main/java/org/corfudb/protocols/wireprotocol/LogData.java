package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;
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
    byte[] data;

    private ByteBuf serializedCache = null;

    private transient final AtomicReference<Object> payload = new AtomicReference<>();

    public Object getPayload(CorfuRuntime runtime) {
        Object value = payload.get();
        if (value == null) {
            synchronized (this.payload) {
                value = this.payload.get();
                if (value == null) {
                    if (data == null) {
                        this.payload.set(null);
                    } else {
                        ByteBuf copyBuf = Unpooled.wrappedBuffer(data);
                        final Object actualValue =
                                Serializers.CORFU.deserialize(copyBuf, runtime);
                        // TODO: Remove circular dependency on logentry.
                        if (actualValue instanceof LogEntry) {
                            ((LogEntry) actualValue).setEntry(this);
                            ((LogEntry) actualValue).setRuntime(runtime);
                        }
                        value = actualValue == null ? this.payload : actualValue;
                        this.payload.set(value);
                        copyBuf.release();
                        data = null;
                    }
                }
            }
        }

        data = null;
        return value;
    }

    @Override
    public synchronized void releaseBuffer() {
        if (serializedCache != null) {
            serializedCache.release();
            if (serializedCache.refCnt() == 0) {
                serializedCache = null;
            }
        }
    }

    @Override
    public synchronized void acquireBuffer() {
        if (serializedCache == null) {
            serializedCache = Unpooled.buffer();
            doSerializeInternal(serializedCache);
        }
        else {
            serializedCache.retain();
        }
    }

    @Override
    public int getSizeEstimate() {
        if (data != null) {
            return data.length;
        }
        return 1;
    }

    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    public LogData(ByteBuf buf) {
        type = ICorfuPayload.fromBuffer(buf, DataType.class);
        if (type == DataType.DATA) {
            data = ICorfuPayload.fromBuffer(buf, byte[].class);
        } else {
            data = null;
        }
        if (type.isMetadataAware()) {
            metadataMap =
                    ICorfuPayload.enumMapFromBuffer(buf,
                            IMetadata.LogUnitMetadataType.class, Object.class);
        } else {
            metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        }
    }

    public LogData(DataType type) {
        this.type = type;
        this.data = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    public LogData(final Object object) {
        this.type = DataType.DATA;
        this.data = null;
        this.payload.set(object);
        if (object instanceof LogEntry) {
            ((LogEntry) object).setEntry(this);
        }
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }
    public LogData(final DataType type, final ByteBuf buf) {
        this.type = type;
        this.data = byteArrayFromBuf(buf);
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    public byte[] byteArrayFromBuf(final ByteBuf buf) {
        ByteBuf readOnlyCopy = buf.asReadOnly();
        readOnlyCopy.resetReaderIndex();
        byte[] outArray = new byte[readOnlyCopy.readableBytes()];
        readOnlyCopy.readBytes(outArray);
        return outArray;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        if (serializedCache != null) {
            serializedCache.resetReaderIndex();
            buf.writeBytes(serializedCache);
        } else {
            doSerializeInternal(buf);
        }
    }

    void doSerializeInternal(ByteBuf buf) {
        ICorfuPayload.serialize(buf, type);
        if (type == DataType.DATA) {
            if (data == null) {
                int lengthIndex = buf.writerIndex();
                buf.writeInt(0);
                Serializers.CORFU.serialize(payload.get(), buf);
                int size = buf.writerIndex() - (lengthIndex + 4);
                buf.writerIndex(lengthIndex);
                buf.writeInt(size);
                buf.writerIndex(lengthIndex + size + 4);
            } else {
                ICorfuPayload.serialize(buf, data);
            }
        }
        if (type.isMetadataAware()) {
            ICorfuPayload.serialize(buf, metadataMap);
        }
    }

    @Override
    public String toString() {
        return "LogData[" + getGlobalAddress() + "]";
    }
}
