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
    final Set<UUID> poisonedStreams;

    @Getter
    final Map<UUID, Set<Long>> conflictSet;

    @Getter
    final Map<UUID, Set<Long>>  writeConflictParams;

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
        this.poisonedStreams = Collections.emptySet();
    }

    /**
     * Constructor for TxResolutionInfo.
     *
     * @param txId transaction identifier
     * @param snapshotTimestamp transaction snapshot timestamp
     * @param conflictMap map of conflict parameters, arranged by stream IDs
     * @param writeConflictParams map of write conflict parameters, arranged by stream IDs
     * @param poisonedStreams set of poisoned streams, which have a conflict against all updates
     */
    public TxResolutionInfo(UUID txId, long snapshotTimestamp, Map<UUID, Set<Long>>
            conflictMap, Map<UUID, Set<Long>> writeConflictParams,
                            Set<UUID> poisonedStreams) {
        this.TXid = txId;
        this.snapshotTimestamp = snapshotTimestamp;
        this.conflictSet = conflictMap;
        this.writeConflictParams = writeConflictParams;
        this.poisonedStreams = poisonedStreams;
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
        ImmutableMap.Builder<UUID, Set<Long>> conflictMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID k = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Long> v = ICorfuPayload.setFromBuffer(buf, Long.class);
            conflictMapBuilder.put(k, v);
        }
        conflictSet = conflictMapBuilder.build();

        // writeConflictParams
        numEntries = buf.readInt();
        ImmutableMap.Builder<UUID, Set<Long>> writeMapBuilder = new ImmutableMap.Builder<>();
        for (int i = 0; i < numEntries; i++) {
            UUID k = ICorfuPayload.fromBuffer(buf, UUID.class);
            Set<Long> v = ICorfuPayload.setFromBuffer(buf, Long.class);
            writeMapBuilder.put(k, v);
        }

        writeConflictParams = writeMapBuilder.build();

        poisonedStreams = ICorfuPayload.setFromBuffer(buf, UUID.class);
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

        ICorfuPayload.serialize(buf, poisonedStreams);
    }

    @Override
    public String toString() {
        return "TXINFO[" + Utils.toReadableID(TXid) + "](ts="
                + snapshotTimestamp + ")";
    }

}

