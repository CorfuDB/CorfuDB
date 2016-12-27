package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.util.Set;
import java.util.UUID;

/**
 * Created by dmalkhi on 12/26/16.
 */
public class TxResolutionInfo implements ICorfuPayload<TxResolutionInfo> {
    /* Latest readstamp of the txn. */
    @Getter
    final Long readTimestamp;

    /*
     * Streams that are in the read set of the txn. The txn can only commit if none of the current offsets in each of
     * streams is greater than readTimestamp.
     */
    @Getter
    final Set<UUID> conflictSet;

    public TxResolutionInfo(ByteBuf buf) {
        readTimestamp = ICorfuPayload.fromBuffer(buf, Long.class);
        conflictSet = ICorfuPayload.setFromBuffer(buf, UUID.class);
    }

    public TxResolutionInfo(long readTS, Set<UUID> streams) {
        this.readTimestamp = readTS;
        this.conflictSet = streams;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, readTimestamp);
        ICorfuPayload.serialize(buf, conflictSet);
    }

}
