package org.corfudb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.Data;

/**
 * A class for dealing with automatically closing allocated bytebufs.
 *
 * <p>Created by mwei on 4/7/17.
 */
@Data
public class AutoClosableByteBuf implements AutoCloseable {

    /** The underlying bytebuf. */
    final ByteBuf buf;

    /** Grab a bytebuf from the default unpooled buffer. */
    public AutoClosableByteBuf() {
        buf = Unpooled.buffer();
    }

    /** Grab a bytebuf from the specified pool. */
    public AutoClosableByteBuf(PooledByteBufAllocator pool) {
        buf = pool.buffer();
    }



    /** {@inheritDoc}
     * Release the underlying buffer.
     * */
    @Override
    public void close() {
        buf.release();
    }
}
