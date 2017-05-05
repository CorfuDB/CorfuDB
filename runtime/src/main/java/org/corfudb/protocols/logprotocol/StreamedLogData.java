package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Temporary class for transition to log data.
 *
 * Created by mwei on 4/6/17.
 */
public class StreamedLogData implements ILogData, ICorfuPayload<StreamedLogData>{

    /** A map of stream data. */
    @Getter
    public final @Nonnull
    Map<UUID, StreamData> streamDataMap;

    /** The metadata for this log entry. */
    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    /** The serialized form, cached during writing only
     * and auto-released by the handle.
     */
    protected ByteBuf serializedForm;

    /** {@inheritDoc} */
    @Override
    public Set<UUID> getStreams() {
        return streamDataMap.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsStream(UUID stream) {
        return streamDataMap.containsKey(stream);
    }

    /** {@inheritDoc} */
    @Override
    public Map<UUID, Long> getBackpointerMap() {
        return streamDataMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().backpointer));
    }

    /** Serialize to the given buffer, using the cached form
     * if possible.
     * @param buf   The buffer to serialize.
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        // If we don't have a cached version, we serialized directly
        if (serializedForm == null) {
            doSerialize(buf);
        } else {
            serializedForm.resetReaderIndex();
            buf.writeBytes(serializedForm);
        }
    }

    /** This class provides a serialization handle, which
     * manages the lifetime of the serialized copy of this
     * entry.
     */
    public static class SerializationHandle implements AutoCloseable {

        /** A reference to the log data. */
        final StreamedLogData data;

        /** Explicitly request the serialized form of this log data
         * which only exists for the lifetime of this handle.
         * @return  The serialized form of this handle.
         */
        public StreamedLogData getSerialized() {
            return data;
        }

        /** Create a new serialized handle with a reference
         * to the log data.
         * @param data  The log data to manage.
         */
        public SerializationHandle(StreamedLogData data)
        {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            data.serializedForm.release();
            data.serializedForm = null;
        }
    }

    public StreamedLogData(long address, Map<UUID, StreamData> streamDataMap) {
        this.setGlobalAddress(address);
        this.streamDataMap = streamDataMap;
        this.metadataMap = new EnumMap<>(LogUnitMetadataType.class);
    }

    public StreamedLogData(ByteBuf buf) {
        ICorfuPayload.fromBuffer(buf, DataType.class);
        streamDataMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamData.class);
        metadataMap = ICorfuPayload.enumMapFromBuffer(buf, LogUnitMetadataType.class, Object.class);
    }

    /** Serialize the data to the given buffer. */
    private void serializedDataToBuffer(ByteBuf buf) {
        ICorfuPayload.serialize(buf, DataType.DATA);
        ICorfuPayload.serialize(buf, streamDataMap);
        ICorfuPayload.serialize(buf, metadataMap);
    }

    /** The payload of an entry with streams is the map. */
    @Override
    public Object getPayload(CorfuRuntime t) {
        return streamDataMap;
    }

    /** Streamed log data always contain data. */
    @Override
    public DataType getType() {
        return DataType.DATA;
    }

    @Override
    public void releaseBuffer() {

    }

    @Override
    public void acquireBuffer() {

    }

}
