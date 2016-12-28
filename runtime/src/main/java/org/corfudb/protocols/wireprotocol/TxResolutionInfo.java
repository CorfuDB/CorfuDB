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
    /* Latest readstamp of the txn. */
    @Getter
    @Setter
    final Long snapshotTimestamp;

    @Getter
    final Map<UUID, Set<Long>> conflictMap;

    public TxResolutionInfo(long readTS, Map<UUID, Set<Long>> conflictMap) {
        this.snapshotTimestamp = readTS;
        this.conflictMap = conflictMap;
    }


    // todo: eventually, deprecate this adaptation constructor
    public TxResolutionInfo(long readTS, Set<UUID> branches) {
        this.snapshotTimestamp = readTS;
        ImmutableMap.Builder<UUID, Set<Long>> conflictMapBuilder = new ImmutableMap.Builder<>();
        if (branches != null)
            branches.forEach(branch -> conflictMapBuilder.put(branch, new HashSet<>()));
        conflictMap = conflictMapBuilder.build();
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
        ImmutableMap.Builder<UUID, Set<Long>> conflictMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID K = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Long> V = ICorfuPayload.setFromBuffer(buf, Long.class);
            conflictMapBuilder.put(K, V);
        }
        conflictMap = conflictMapBuilder.build();
    }

    /**
     * fast , specialized serialization of object into ByteBuf
     * @param buf
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeLong(snapshotTimestamp);
        buf.writeInt(conflictMap.size());
        conflictMap.entrySet().stream().forEach(x -> {
            ICorfuPayload.serialize(buf, x.getKey());
            ICorfuPayload.serialize(buf, x.getValue());
        });
    }
}

