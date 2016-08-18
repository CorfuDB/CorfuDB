package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/18/15.
 */
public interface IMetadata {

    public static Map<Byte, LogUnitMetadataType> metadataTypeMap =
            Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                    .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));

    EnumMap<IMetadata.LogUnitMetadataType, Object> getMetadataMap();

    /**
     * Get the streams that belong to this write.
     *
     * @return A set of streams that belong to this write.
     */
    @SuppressWarnings("unchecked")
    default Set<UUID> getStreams() {
        return (Set<UUID>) getMetadataMap().getOrDefault(
                LogUnitMetadataType.STREAM,
                Collections.EMPTY_SET);
    }

    /**
     * Set the streams that belong to this write.
     *
     * @param streams The set of streams that will belong to this write.
     */
    default void setStreams(Set<UUID> streams) {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.STREAM, streams);
    }

    /**
     * Get the rank of this write.
     *
     * @return The rank of this write.
     */
    @SuppressWarnings("unchecked")
    default Long getRank() {
        return (Long) getMetadataMap().getOrDefault(IMetadata.LogUnitMetadataType.RANK,
                0L);
    }

    /**
     * Set the rank of this write.
     *
     * @param rank The rank of this write.
     */
    default void setRank(Long rank) {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.RANK, rank);
    }

    /**
     * Get the logical stream addresses that belong to this write.
     *
     * @return A list of logical stream addresses that belong to this write.
     */
    @SuppressWarnings("unchecked")
    default List<Long> getLogicalAddresses() {
        return (List<Long>) getMetadataMap().getOrDefault(IMetadata.LogUnitMetadataType.STREAM_ADDRESS,
                Collections.EMPTY_LIST);
    }

    /**
     * Set the logical stream addresses that belong to this write.
     *
     * @param streams The set of logical stream addresses that will belong to this write.
     */
    default void setLogicalAddresses(List<Long> streams) {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.STREAM_ADDRESS, streams);
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
        return (Long) getMetadataMap().get(LogUnitMetadataType.GLOBAL_ADDRESS);
    }

    @RequiredArgsConstructor
    public enum LogUnitMetadataType implements ITypedEnum {
        STREAM(0, new TypeToken<Set<UUID>>() {}),
        RANK(1, TypeToken.of(Long.class)),
        STREAM_ADDRESS(2, TypeToken.of(Long.class)),
        BACKPOINTER_MAP(3, new TypeToken<Map<UUID, Long>>() {}),
        GLOBAL_ADDRESS(4, TypeToken.of(Long.class))
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
}
