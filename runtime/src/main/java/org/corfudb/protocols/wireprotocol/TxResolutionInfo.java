package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

/**
 * Created by dmalkhi on 12/26/16.
 */
public class TxResolutionInfo implements ICorfuPayload<TxResolutionInfo> {
    /* snapshot timestamp of the txn. */
    @Getter
    @Setter
    final Long snapshotTimestamp;

    @Getter
    final Map<UUID, Set<Integer>> conflictSet;

    @Getter
    final Set<Integer> writeConflictParams; // todo: should we group them by streams??

    public TxResolutionInfo(long snapshotTS, Map<UUID, Set<Integer>> conflictMap, Set<Integer> writeConflictParams) {
        this.snapshotTimestamp = snapshotTS;
        this.conflictSet = conflictMap;
        this.writeConflictParams = writeConflictParams;
    }


    public TxResolutionInfo(long readTS, Set<UUID> streams,  Set<Integer> writeConflictParams) {
        this.snapshotTimestamp = readTS;
        ImmutableMap.Builder<UUID, Set<Integer>> conflictMapBuilder = new ImmutableMap.Builder<>();
        if (streams != null)
            streams.forEach(streamID -> conflictMapBuilder.put(streamID, new HashSet<>()));
        conflictSet = conflictMapBuilder.build();
        this.writeConflictParams = writeConflictParams;
    }

    /**
     * fast, specialized deserialization constructor, from a ByteBuf to this object
     *
     * The first entry is a long, the snapshot timestamp.
     * The second is an int, the size of the map.
     * Next, entries are serialized one by one, first the key, then each value, itself a set of objects.
     *
     * @param buf        The buffer to deserialize.
     */
    public TxResolutionInfo(ByteBuf buf) {
        snapshotTimestamp = buf.readLong();
        int numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<Integer>> conflictMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID K = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Integer> V = ICorfuPayload.setFromBuffer(buf, Integer.class);
            conflictMapBuilder.put(K, V);
        }
        conflictSet = conflictMapBuilder.build();
        writeConflictParams = ICorfuPayload.setFromBuffer(buf, Integer.class);
    }

    /**
     * fast , specialized serialization of object into ByteBuf
     * @param buf
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeLong(snapshotTimestamp);
        buf.writeInt(conflictSet.size());
        conflictSet.entrySet().stream().forEach(x -> {
            ICorfuPayload.serialize(buf, x.getKey());
            ICorfuPayload.serialize(buf, x.getValue());
        });
        ICorfuPayload.serialize(buf, writeConflictParams);
    }
}

