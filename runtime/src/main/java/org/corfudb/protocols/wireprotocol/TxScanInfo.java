package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * Created by mwei on 7/12/17.
 */
public class TxScanInfo implements ICorfuPayload<TxScanInfo> {

    @Getter
    long startPos;
    @Getter
    long endPos;

    public TxScanInfo(long startPos, long endPos) {
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public TxScanInfo(ByteBuf buf) {
        startPos = ICorfuPayload.fromBuffer(buf, Long.class);
        endPos = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, startPos);
        ICorfuPayload.serialize(buf, endPos);
    }
}
