package org.corfudb.protocols.wireprotocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Data;

public class BackpointerResponse implements ICorfuPayload<BackpointerResponse> {

    final ByteBuf buf;

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeBytes(this.buf);
        this.buf.release();
    }

    @Data
    public static class BackpointerElement {
        final UUID stream;
        final long address;
    }


    final int ELEMENT_SIZE = 16 + 8;
    public BackpointerResponse(int size) {
        buf = PooledByteBufAllocator.DEFAULT.buffer(ELEMENT_SIZE * size);
        buf.writeInt(size);
    }

    public void add(UUID stream, long element) {
        buf.writeLong(stream.getMostSignificantBits());
        buf.writeLong(stream.getLeastSignificantBits());
        buf.writeLong(element);
    }
}
