package org.corfudb.protocols.wireprotocol;

import com.esotericsoftware.kryo.NotNull;
import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.corfudb.protocols.logprotocol.CheckpointEntry;

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

import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINT_ID;
import static org.corfudb.protocols.wireprotocol.IMetadata.LogUnitMetadataType.CHECKPOINT_TYPE;

/**
 * Created by mwei on 9/18/15.
 */
public interface IMetadata {

    Map<Byte, LogUnitMetadataType> metadataTypeMap =
            Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                    .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));

    EnumMap<IMetadata.LogUnitMetadataType, Object> getMetadataMap();

    /**
     * Get the streams that belong to this append.
     *
     * @return A set of streams that belong to this append.
     */
    @SuppressWarnings("unchecked")
    default Set<UUID> getStreams() {
        return (Set<UUID>) ((Map<UUID, Long>)getMetadataMap().getOrDefault(
                LogUnitMetadataType.BACKPOINTER_MAP, Collections.emptyMap())).keySet();
    }

    /**
     * Get whether or not this entry contains a given stream.
     * @param stream    The stream to check.
     * @return          True, if the entry contains the given stream.
     */
    default boolean containsStream(UUID stream) {
        return  getBackpointerMap().keySet().contains(stream);
    }

    /**
     * Get the rank of this append.
     *
     * @return The rank of this append.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    default DataRank getRank() {
        return (DataRank) getMetadataMap().getOrDefault(LogUnitMetadataType.RANK,
                null);
    }

    /**
     * Set the rank of this append.
     *
     * @param rank The rank of this append.
     */
    default void setRank(@Nullable DataRank rank) {
        EnumMap<LogUnitMetadataType, Object> map = getMetadataMap();
        if (rank != null) {
            map.put(LogUnitMetadataType.RANK, rank);
        } else {
            if (map.containsKey(LogUnitMetadataType.RANK)) {
                map.remove(LogUnitMetadataType.RANK);
            }
        }
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

    @SuppressWarnings("unchecked")
    default Long getGlobalAddress() {
        if (getMetadataMap() == null || getMetadataMap().get(LogUnitMetadataType.GLOBAL_ADDRESS) == null) {
            return -1L;
        }
        return Optional.ofNullable((Long) getMetadataMap().get(LogUnitMetadataType.GLOBAL_ADDRESS)).orElse((long) -1);
    }

    default void clearCommit() {
        getMetadataMap().put(LogUnitMetadataType.COMMIT, false);
    }

    default void setCommit() {
        getMetadataMap().put(LogUnitMetadataType.COMMIT, true);
    }

    default boolean hasCheckpointMetadata() {
        return getCheckpointType() != null &&
               getCheckpointID() != null;
    }

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
    default UUID getCheckpointID() {
        return (UUID) getMetadataMap().getOrDefault(LogUnitMetadataType.CHECKPOINT_ID,
                null);
    }

    default void setCheckpointID(UUID ID) {
        getMetadataMap().put(CHECKPOINT_ID, ID);
    }

    @RequiredArgsConstructor
    public enum LogUnitMetadataType implements ITypedEnum {
        RANK(1, TypeToken.of(DataRank.class)),
        BACKPOINTER_MAP(3, new TypeToken<Map<UUID, Long>>() {}),
        GLOBAL_ADDRESS(4, TypeToken.of(Long.class)),
        COMMIT(5, TypeToken.of(Boolean.class)),
        CHECKPOINT_TYPE(6, TypeToken.of(CheckpointEntry.CheckpointEntryType.class)),
        CHECKPOINT_ID(7, TypeToken.of(UUID.class))
        ;
        final int type;
        @Getter
        final TypeToken<?> componentType;

        public byte asByte() {
            return (byte) type;
        }

        public static Map<Byte, LogUnitMetadataType> typeMap =
                Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                        .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));
    }

    @Value
    @AllArgsConstructor
    class DataRank implements Comparable<DataRank> {
        public long rank;
        @NotNull
        public UUID uuid;

        public DataRank(long rank) {
            this(rank, UUID.randomUUID());
        }

        public DataRank buildHigherRank() {
            return new DataRank(rank+1, uuid);
        }

        @Override
        public int compareTo(DataRank o) {
            int rankCompared = Long.compare(this.rank, o.rank);
            if (rankCompared==0) {
                return uuid.compareTo(o.getUuid());
            } else {
                return rankCompared;
            }
        }
    }

    
}
