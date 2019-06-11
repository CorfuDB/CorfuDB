package org.corfudb.infrastructure.log;

import org.corfudb.format.Types;

import java.nio.ByteBuffer;

/**
 * Created by Maithem on 6/11/19.
 */
public interface Compressor {

    Types.CompressionType getType();
    ByteBuffer encode(ByteBuffer buffer);
    ByteBuffer decode(ByteBuffer buffer);
}
