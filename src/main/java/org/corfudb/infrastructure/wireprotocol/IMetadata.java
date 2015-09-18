package org.corfudb.infrastructure.wireprotocol;

import org.corfudb.infrastructure.NettyLogUnitServer;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 9/18/15.
 */
public interface IMetadata {

    EnumMap<NettyLogUnitServer.LogUnitMetadataType, Object> getMetadataMap();

    /** Get the streams that belong to this write.
     *
     * @return A set of streams that belong to this write.
     */
    @SuppressWarnings("unchecked")
    default Set<UUID> getStreams()
    {
        return (Set<UUID>) getMetadataMap().getOrDefault(NettyLogUnitServer.LogUnitMetadataType.STREAM,
                Collections.EMPTY_SET);
    }

    /** Set the streams that belong to this write.
     *
     * @param streams The set of streams that will belong to this write.
     */
    default void setStreams(Set<UUID> streams)
    {
        getMetadataMap().put(NettyLogUnitServer.LogUnitMetadataType.STREAM, streams);
    }

    /** Get the rank of this write.
     *
     * @return The rank of this write.
     */
    @SuppressWarnings("unchecked")
    default Long getRank()
    {
        return (Long) getMetadataMap().getOrDefault(NettyLogUnitServer.LogUnitMetadataType.RANK,
                0L);
    }

    /** Set the rank of this write.
     *
     * @param rank The rank of this write.
     */
    default void setRank(Long rank)
    {
        getMetadataMap().put(NettyLogUnitServer.LogUnitMetadataType.RANK, rank);
    }

}
