package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.util.Utils;

/**
 * Created by dmalkhi on 12/26/16.
 */
public class TxResolutionInfo implements ICorfuPayload<TxResolutionInfo> {

    @Getter
    @Setter
    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    UUID TXid; // transaction ID, mostly for debugging purposes

    /* snapshot timestamp of the txn. */
    @Getter
    @Setter
    Long snapshotTimestamp;

    /** A set of poisoned streams, which have a conflict against all updates. */

    @Getter
    final Map<UUID, Set<byte[]>> conflictSet;

    @Getter
    final Map<UUID, Set<byte[]>>  writeConflictParams;

    /**
     * Constructor for TxResolutionInfo.
     *
     * @param txId transaction identifier
     * @param snapshotTimestamp transaction snapshot timestamp
     */
    public TxResolutionInfo(UUID txId, long snapshotTimestamp) {
        this.TXid = txId;
        this.snapshotTimestamp = snapshotTimestamp;
        this.conflictSet = Collections.emptyMap();
        this.writeConflictParams = Collections.emptyMap();
    }

    /**
     * Constructor for TxResolutionInfo.
     *
     * @param txId transaction identifier
     * @param snapshotTimestamp transaction snapshot timestamp
     * @param conflictMap map of conflict parameters, arranged by stream IDs
     * @param writeConflictParams map of write conflict parameters, arranged by stream IDs
     */
    public TxResolutionInfo(UUID txId, long snapshotTimestamp, Map<UUID, Set<byte[]>>
            conflictMap, Map<UUID, Set<byte[]>> writeConflictParams) {
        this.TXid = txId;
        this.snapshotTimestamp = snapshotTimestamp;
        this.conflictSet = conflictMap;
        this.writeConflictParams = writeConflictParams;
    }

    /**
     * fast, specialized deserialization constructor, from a ByteBuf to this object
     *
     * <p>The first entry is a long, the snapshot timestamp.
     * The second is an int, the size of the map.
     * Next, entries are serialized one by one, first the key, then each value,
     * itself a set of objects.</p>
     *
     * @param buf        The buffer to deserialize.
     */
    public TxResolutionInfo(ByteBuf buf) {
        TXid = ICorfuPayload.fromBuffer(buf, UUID.class);
        snapshotTimestamp = buf.readLong();

        // conflictSet
        int numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<byte[]>> conflictMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID k = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<byte[]> v = ICorfuPayload.setFromBuffer(buf, byte[].class);
            conflictMapBuilder.put(k, v);
        }
        conflictSet = conflictMapBuilder.build();

        // writeConflictParams
        numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<byte[]>> writeMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID k = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<byte[]> v = ICorfuPayload.setFromBuffer(buf, byte[].class);
            writeMapBuilder.put(k, v);
        }

        writeConflictParams = writeMapBuilder.build();
    }

    /**
     * fast , specialized serialization of object into ByteBuf.
     *
     * @param buf The buffer to serialize object into
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
        return "TXINFO[" + Utils.toReadableId(TXid) + "](ts="
                + snapshotTimestamp + ")";
    }

}

