package org.corfudb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.io.Closeable;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * Created by mwei on 9/15/15.
 */
@Slf4j
public class SizeBufferPool {

    @Data
    public class PooledSizedBuffer
    {
        final ByteBuf buffer;
    }

    private final PooledByteBufAllocator bufferPool;
    private final int initialSize;

    public SizeBufferPool(int initialSize)
    {
        this.initialSize = initialSize;
        bufferPool = new PooledByteBufAllocator(true);
    }

    public PooledSizedBuffer getSizedBuffer()
    {
       ByteBuf b = bufferPool.directBuffer(initialSize);
       return new PooledSizedBuffer(b);
    }

}
