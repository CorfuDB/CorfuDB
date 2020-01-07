package org.corfudb.common.compression;

import java.nio.ByteBuffer;

/**
 *
 * Created by Maithem on 11/6/19.
 */
public class NoCompression implements Codec {
    private static NoCompression INSTANCE = new NoCompression();


    public static Codec getInstance() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        return uncompressed;
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        return compressed;
    }
}
