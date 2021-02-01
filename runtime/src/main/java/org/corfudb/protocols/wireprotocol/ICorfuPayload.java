package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

public interface ICorfuPayload {

    @FunctionalInterface
    interface PayloadConstructor<T> {
        T construct(ByteBuf buf);
    }

    void doSerialize(ByteBuf buf);
}
