package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINTED_STREAM_ID;
import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINTED_STREAM_START_LOG_ADDRESS;
import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINT_ID;
import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINT_TYPE;

/**
 * Created by mwei on 9/18/15.
 */
public interface IMetadata {

    EnumMap<IMetadata.LogUnitMetadataType, Object> getMetadataMap();

    /**
     * Get the streams that belong to this append.
     *
     * @return A set of streams that belong to this append.
     */
    @SuppressWarnings("unchecked")
    default Set<UUID> getStreams() {
        return ((Map<UUID, Long>) getMetadataMap().getOrDefault(
                LogUnitMetadataType.BACKPOINTER_MAP, Collections.emptyMap())).keySet();
    }

    /**
     * Get whether or not this entry contains a given stream.
     *
     * @param stream The stream to check.
     * @return True, if the entry contains the given stream.
     */
    default boolean containsStream(UUID stream) {
        return getBackpointerMap().containsKey(stream);
    }

    @SuppressWarnings("unchecked")
    default Map<UUID, Long> getBackpointerMap() {
        return (Map<UUID, Long>) getMetadataMap().getOrDefault(LogUnitMetadataType.BACKPOINTER_MAP,
                Collections.EMPTY_MAP);
    }

    default void setBackpointerMap(Map<UUID, Long> backpointerMap) {
        getMetadataMap().put(LogUnitMetadataType.BACKPOINTER_MAP, backpointerMap);
    }

    default void setGlobalAddress(Long address) {
        getMetadataMap().put(LogUnitMetadataType.GLOBAL_ADDRESS, address);
    }

    default void setEpoch(Long epoch) {
        getMetadataMap().put(LogUnitMetadataType.EPOCH, epoch);
    }

    default void setClientId(UUID clientId) {
        getMetadataMap().put(LogUnitMetadataType.CLIENT_ID, clientId);
    }

    @Nullable
    default UUID getClientId() {
        return (UUID) getMetadataMap().getOrDefault(LogUnitMetadataType.CLIENT_ID, null);
    }

    default void setThreadId(Long threadId) {
        getMetadataMap().put(LogUnitMetadataType.THREAD_ID, threadId);
    }

    @Nullable
    default Long getThreadId() {
        return (Long) getMetadataMap().getOrDefault(LogUnitMetadataType.THREAD_ID, null);
    }

    /**
     * Get Log's global address (global tail).
     *
     * @return global address
     */
    default Long getGlobalAddress() {
        return Optional.ofNullable((Long) getMetadataMap()
                .get(LogUnitMetadataType.GLOBAL_ADDRESS)).orElse(Address.NON_ADDRESS);
    }

    /**
     * Get Log's epoch.
     *
     * @return epoch.
     */
    default Long getEpoch() {
        if (getMetadataMap() == null
                || getMetadataMap().get(LogUnitMetadataType.EPOCH) == null) {
            return Layout.INVALID_EPOCH;
        }
        return Optional.ofNullable((Long) getMetadataMap()
                .get(LogUnitMetadataType.EPOCH)).orElse(Layout.INVALID_EPOCH);
    }

    // TODO(Maithem): replace getGlobalAddress and getEpoch with getToken
    default Token getToken() {
        return new Token(getEpoch(), getGlobalAddress());
    }

    default boolean hasCheckpointMetadata() {
        return getCheckpointType() != null && getCheckpointId() != null;
    }

    /**
     * Get checkpoint type.
     */
    @Nullable
    default CheckpointEntry.CheckpointEntryType getCheckpointType() {
        return (CheckpointEntry.CheckpointEntryType) getMetadataMap()
                .getOrDefault(LogUnitMetadataType.CHECKPOINT_TYPE,
                        null);
    }

    default void setCheckpointType(CheckpointEntry.CheckpointEntryType type) {
        getMetadataMap().put(CHECKPOINT_TYPE, type);
    }

    @Nullable
    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    default UUID getCheckpointId() {
        return (UUID) getMetadataMap().getOrDefault(LogUnitMetadataType.CHECKPOINT_ID,
                null);
    }

    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    default void setCheckpointId(UUID id) {
        getMetadataMap().put(CHECKPOINT_ID, id);
    }

    default UUID getCheckpointedStreamId() {
        return (UUID) getMetadataMap().getOrDefault(LogUnitMetadataType.CHECKPOINTED_STREAM_ID,
                null);
    }

    default void setCheckpointedStreamId(UUID Id) {
        getMetadataMap().put(CHECKPOINTED_STREAM_ID, Id);
    }

    /**
     * Returns the tail of the checkpointed stream at the time of taking the checkpoint snapshot.
     */
    default Long getCheckpointedStreamStartLogAddress() {
        return (Long) getMetadataMap()
                .getOrDefault(LogUnitMetadataType.CHECKPOINTED_STREAM_START_LOG_ADDRESS,
                        Address.NO_BACKPOINTER);
    }

    default void setCheckpointedStreamStartLogAddress(Long startLogAddress) {
        getMetadataMap().put(CHECKPOINTED_STREAM_START_LOG_ADDRESS, startLogAddress);
    }

    /**
     * Set the codec type to encode/decode the payload.
     *
     * @param type codec type (NONE, LZ4, ZSTD...)
     */
    default void setPayloadCodecType(Codec.Type type) {
        getMetadataMap().put(LogUnitMetadataType.PAYLOAD_CODEC, type);
    }

    /**
     * Get the payloads encoder/decoder type.
     *
     * @return codec type (NONE, LZ4, ZSTD...)
     */
    default Codec.Type getPayloadCodecType() {
        return (Codec.Type) getMetadataMap().getOrDefault(LogUnitMetadataType.PAYLOAD_CODEC, Codec.Type.NONE);
    }

    /**
     * Check if payload has a specified codec.
     *
     * @return true, if codec is set for this payload. False, otherwise.
     */
    default boolean hasPayloadCodec() {
        return getPayloadCodecType() != Codec.Type.NONE;
    }

    enum LogUnitMetadataType implements ITypedEnum {
        BACKPOINTER_MAP(3, new TypeToken<Map<UUID, Long>>() {}),
        GLOBAL_ADDRESS(4, TypeToken.of(Long.class)),
        CHECKPOINT_TYPE(6, TypeToken.of(CheckpointEntry.CheckpointEntryType.class)),
        CHECKPOINT_ID(7, TypeToken.of(UUID.class)),
        CHECKPOINTED_STREAM_ID(8, TypeToken.of(UUID.class)),
        CHECKPOINTED_STREAM_START_LOG_ADDRESS(9, TypeToken.of(Long.class)),
        CLIENT_ID(10, TypeToken.of(UUID.class)),
        THREAD_ID(11, TypeToken.of(Long.class)),
        EPOCH(12, TypeToken.of(Long.class)),
        PAYLOAD_CODEC(13, TypeToken.of(Codec.Type.class));
        final int type;
        @Getter
        final TypeToken<?> componentType;

        private LogUnitMetadataType(int type, TypeToken<?> componentType) {
            this.type = type;
            this.componentType = componentType;
        }

        public byte asByte() {
            return (byte) type;
        }

        public static Map<Byte, LogUnitMetadataType> typeMap =
                Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                        .collect(Collectors.toMap(LogUnitMetadataType::asByte,
                                Function.identity()));
    }
}
