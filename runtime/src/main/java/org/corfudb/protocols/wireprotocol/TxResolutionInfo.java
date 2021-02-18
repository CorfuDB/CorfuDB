package org.corfudb.protocols.wireprotocol;

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
public class TxResolutionInfo {

    @Getter
    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    final UUID TXid; // transaction ID, mostly for debugging purposes

    /* snapshot timestamp of the txn. */
    @Getter
    @Setter
    Token snapshotTimestamp;

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
    public TxResolutionInfo(UUID txId, Token snapshotTimestamp) {
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
    public TxResolutionInfo(UUID txId, Token snapshotTimestamp, Map<UUID, Set<byte[]>>
            conflictMap, Map<UUID, Set<byte[]>> writeConflictParams) {
        this.TXid = txId;
        this.snapshotTimestamp = snapshotTimestamp;
        this.conflictSet = conflictMap;
        this.writeConflictParams = writeConflictParams;
    }

    @Override
    public String toString() {
        return "TXINFO[" + Utils.toReadableId(TXid) + "](ts="
                + snapshotTimestamp + ")";
    }
}

