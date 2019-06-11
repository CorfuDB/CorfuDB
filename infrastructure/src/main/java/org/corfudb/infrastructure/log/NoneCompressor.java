package org.corfudb.infrastructure.log;

import org.corfudb.format.Types;

import java.nio.ByteBuffer;

/**
 * Created by Maithem on 6/11/19.
 */
public class NoneCompressor implements Compressor {

    @Override
    public Types.CompressionType getType() {
        return Types.CompressionType.NONE;
    }

    @Override
    public ByteBuffer encode(ByteBuffer buffer) {
        return buffer;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        return buffer;
    }
}
