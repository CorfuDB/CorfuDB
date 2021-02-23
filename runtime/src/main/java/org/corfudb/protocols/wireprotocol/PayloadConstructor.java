package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
public
interface PayloadConstructor<T> {
    T construct(ByteBuf buf);
}