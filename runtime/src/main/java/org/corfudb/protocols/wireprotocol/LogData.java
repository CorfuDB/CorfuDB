package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.util.serializer.Serializers;

/**
 * Created by mwei on 8/15/16.
 */
@Slf4j
public class LogData implements ICorfuPayload<LogData>, IMetadata, ILogData {

    public static final int NOT_KNOWN = -1;

    @Getter
    final DataType type;

    @Getter
    byte[] data;

    private ByteBuf serializedCache = null;

    private int lastKnownSize = NOT_KNOWN;

    private final transient AtomicReference<Object> payload = new AtomicReference<>();

    public static LogData getTrimmed(long address) {
        LogData logData = new LogData(DataType.TRIMMED);
        logData.setGlobalAddress(address);
        return logData;
    }

    public static LogData getHole(long address) {
        LogData logData = new LogData(DataType.HOLE);
        logData.setGlobalAddress(address);
        return logData;
    }

    public static LogData getHole(Token token) {
        LogData logData = new LogData(DataType.HOLE);
        logData.useToken(token);
        return logData;
    }

    public static LogData getEmpty(long address) {
        LogData logData = new LogData(DataType.EMPTY);
        logData.setGlobalAddress(address);
        return logData;
    }

    /**
     * Return the payload.
     */
    public Object getPayload(CorfuRuntime runtime) {
        Object value = payload.get();
        if (value == null) {
            synchronized (this.payload) {
                value = this.payload.get();
                if (value == null) {
                    if (data == null) {
                        this.payload.set(null);
                    } else {
                        ByteBuf serializedBuf = Unpooled.wrappedBuffer(data);
                        if (hasPayloadCodec()) {
                            // if the payload has a codec we need to decode it before deserialization
                            ByteBuf compressedBuf = ICorfuPayload.fromBuffer(data, ByteBuf.class);
                            byte[] compressedArrayBuf= new byte[compressedBuf.readableBytes()];
                            compressedBuf.readBytes(compressedArrayBuf);
                            serializedBuf = Unpooled.wrappedBuffer(getPayloadCodecType()
                                    .getInstance().decompress(ByteBuffer.wrap(compressedArrayBuf)));
                        }

                        final Object actualValue;
                        try {
                            actualValue =
                                    Serializers.CORFU.deserialize(serializedBuf, runtime);

                            if (actualValue instanceof LogEntry) {
                                ((LogEntry) actualValue).setGlobalAddress(getGlobalAddress());
                                ((LogEntry) actualValue).setRuntime(runtime);
                            }
                            value = actualValue == null ? this.payload : actualValue;
                            this.payload.set(value);
                            lastKnownSize = data.length;
                        } catch (Throwable throwable) {
                            log.error("Exception caught at address {}, {}, {}",
                                    getGlobalAddress(), getStreams(), getType());
                            log.error("Raw data buffer {}",
                                    serializedBuf.resetReaderIndex().toString(Charset.defaultCharset()));
                            throw throwable;
                        } finally {
                            serializedBuf.release();
                            data = null;
                        }
                    }
                }
            }
        }

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
        } else {
            serializedCache.retain();
        }
    }

    @Override
    public int getSizeEstimate() {
        byte[] tempData = data;
        if (tempData != null) {
            return tempData.length;
        } else if (lastKnownSize != NOT_KNOWN) {
            return lastKnownSize;
        }
        log.warn("getSizeEstimate: LogData size estimate is defaulting to 1,"
                + " this might cause leaks in the cache!");
        return 1;
    }

    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    /**
     * Return the payload.
     */
    public LogData(ByteBuf buf) {
        type = ICorfuPayload.fromBuffer(buf, DataType.class);
        if (type == DataType.DATA) {
            data = ICorfuPayload.fromBuffer(buf, byte[].class);
        } else {
            data = null;
        }

        if (type.isMetadataAware()) {
            metadataMap = ICorfuPayload.enumMapFromBuffer(buf, IMetadata.LogUnitMetadataType.class);
        } else {
            metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        }
    }

    /**
     * Constructor for generating LogData.
     *
     * @param type The type of log data to instantiate.
     */
    public LogData(DataType type) {
        this.type = type;
        this.data = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    public LogData(DataType type, final Object object, final int codecId) {
        this(type, object, Codec.getCodecTypeById(codecId));
    }

    /**
     * Constructor for generating LogData.
     *
     * @param type The type of log data to instantiate.
     * @param object The actual data/value
     */
    public LogData(DataType type, final Object object) {
        if (object instanceof ByteBuf) {
            this.type = type;
            this.data = byteArrayFromBuf((ByteBuf) object);
            this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        } else {
            this.type = type;
            this.data = null;
            this.payload.set(object);
            this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
            if (object instanceof CheckpointEntry) {
                CheckpointEntry cp = (CheckpointEntry) object;
                setCheckpointType(cp.getCpType());
                setCheckpointId(cp.getCheckpointId());
                setCheckpointedStreamId(cp.getStreamId());
                setCheckpointedStreamStartLogAddress(
                        Long.parseLong(cp.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)));
            }
        }
    }

    /**
     * Constructor for generating LogData.
     *
     * @param type The type of log data to instantiate.
     * @param object The actual data/value
     * @param codecType The encoder/decoder type
     */
    public LogData(DataType type, final Object object, final Codec.Type codecType) {
        this(type, object);
        setPayloadCodecType(codecType);
    }

    /**
     * Assign a given token to this log data.
     *
     * @param token the token to use
     */
    @Override
    public void useToken(IToken token) {
        setGlobalAddress(token.getSequence());
        setEpoch(token.getEpoch());
        if (token.getBackpointerMap().size() > 0) {
            setBackpointerMap(token.getBackpointerMap());
        }
        if (payload.get() instanceof LogEntry) {
            ((LogEntry) payload.get()).setGlobalAddress(token.getSequence());
        }
    }

    /**
     * Return a byte array from buffer.
     *
     * @param buf The buffer to read from
     */
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
                if (hasPayloadCodec()) {
                    // if the payload has a codec we need to also compress the payload
                    ByteBuf serializeBuf = Unpooled.buffer();
                    Serializers.CORFU.serialize(payload.get(), serializeBuf);
                    doCompressInternal(serializeBuf, buf);
                } else {
                    Serializers.CORFU.serialize(payload.get(), buf);
                }
                int size = buf.writerIndex() - (lengthIndex + 4);
                buf.writerIndex(lengthIndex);
                buf.writeInt(size);
                buf.writerIndex(lengthIndex + size + 4);
                lastKnownSize = size;
            } else {
                ICorfuPayload.serialize(buf, data);
                lastKnownSize = data.length;
            }
        }

        if (type.isMetadataAware()) {
            ICorfuPayload.serialize(buf, metadataMap);
        }
    }

    private void doCompressInternal(ByteBuf bufData, ByteBuf buf) {
        ByteBuffer wrappedByteBuf = ByteBuffer.wrap(bufData.array(), 0, bufData.readableBytes());
        ByteBuffer compressedBuf = getPayloadCodecType().getInstance().compress(wrappedByteBuf);
        ICorfuPayload.serialize(buf, Unpooled.wrappedBuffer(compressedBuf));
    }

    /**
     * LogData are considered equals if clientId and threadId are equal.
     * Here, it means or both of them are null or both of them are the same.
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof LogData)) {
            return false;
        } else {
            LogData other = (LogData) o;
            if (compareTo(other) == 0) {
                boolean sameClientId = getClientId() == null ? other.getClientId() == null :
                        getClientId().equals(other.getClientId());
                boolean sameThreadId = getThreadId() == null ? other.getThreadId() == null :
                        getThreadId().equals(other.getThreadId());

                return sameClientId && sameThreadId;
            }

            return false;
        }
    }

    @Override
    public String toString() {
        return "LogData[" + getGlobalAddress() + "]";
    }

    /**
     * Verify that max payload is enforced for the specified limit.
     *
     * @param limit Max write limit.
     */
    public void checkMaxWriteSize(int limit) {
        try (ILogData.SerializationHandle sh = this.getSerializedForm()) {
            if (limit != 0 && getSizeEstimate() > limit) {
                throw new WriteSizeException(getSizeEstimate(), limit);
            }
        }
    }
}
