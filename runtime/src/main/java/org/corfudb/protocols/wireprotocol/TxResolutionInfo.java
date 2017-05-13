package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.util.Utils;

import java.util.*;

/**
 * Created by dmalkhi on 12/26/16.
 */
public class TxResolutionInfo implements ICorfuPayload<TxResolutionInfo> {

    @Getter
    @Setter
    UUID TXid; // transaction ID, mostly for debugging purposes

    /* snapshot timestamp of the txn. */
    @Getter
    @Setter
    Long snapshotTimestamp;

    @Getter
    final Map<UUID, Set<Integer>> conflictSet;

    @Getter
    final Map<UUID, Set<Integer>>  writeConflictParams;

    public TxResolutionInfo(UUID TXid, long snapshotTS) {
        this.TXid = TXid;
        this.snapshotTimestamp = snapshotTS;
        this.conflictSet = Collections.emptyMap();
        this.writeConflictParams = Collections.emptyMap();
    }

    public TxResolutionInfo(UUID TXid, long snapshotTS, Map<UUID, Set<Integer>>
            conflictMap, Map<UUID, Set<Integer>> writeConflictParams) {
        this.TXid = TXid;
        this.snapshotTimestamp = snapshotTS;
        this.conflictSet = conflictMap;
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
        TXid = ICorfuPayload.fromBuffer(buf, UUID.class);
        snapshotTimestamp = buf.readLong();

        // conflictSet
        int numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<Integer>> conflictMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID K = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Integer> V = ICorfuPayload.setFromBuffer(buf, Integer.class);
            conflictMapBuilder.put(K, V);
        }
        conflictSet = conflictMapBuilder.build();

        // writeConflictParams
        numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<Integer>> writeMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID K = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Integer> V = ICorfuPayload.setFromBuffer(buf, Integer.class);
            writeMapBuilder.put(K, V);
        }
        writeConflictParams = writeMapBuilder.build();
    }

    /**
     * fast , specialized serialization of object into ByteBuf
     * @param buf
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, TXid);
        buf.writeLong(snapshotTimestamp);

        // conflictSet
        buf.writeInt(conflictSet.size());
        conflictSet.entrySet().stream().forEach(x -> {
            ICorfuPayload.serialize(buf, x.getKey());
            ICorfuPayload.serialize(buf, x.getValue());
        });

        // writeConflictParams
        buf.writeInt(writeConflictParams.size());
        writeConflictParams.entrySet().stream().forEach(x -> {
            ICorfuPayload.serialize(buf, x.getKey());
            ICorfuPayload.serialize(buf, x.getValue());
        });
    }

    @Override
    public String toString() {
        return "TXINFO[" + Utils.toReadableID(TXid) + "](ts="
                + snapshotTimestamp + ")";
    }

}

