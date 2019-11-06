package org.corfudb.infrastructure.log.Compression;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 *
 * An interface that defines generic compress/decompress functionality
 *
 * Created by Maithem on 11/6/19.
 */

public interface Codec {

    /**
     * Supported codecs
     */
    enum Type {
        None(NoCompression::getInstance),
        LZ4(LZ4Compression::getInstance);
        private Supplier<Codec> func;
        Type(Supplier<Codec> func) {
            this.func = func;
        }

        public Codec getInstance() {
            return func.get();
        }
    }

    /**
     * Returns the compressed ByteBuffer as a ByteBuffer with the size of
     * the uncompressed buffer as the first 4-bytes.
     * |     4 bytes           |   compressed buffer  |
     *    uncompressed length       compressed bytes
     */
    ByteBuffer compress(ByteBuffer uncompressed);

    /**
     * Strips the first 4-bytes and decompresses the remaining of the
     * buffer into a ByteBuffer
     *
     */
    ByteBuffer decompress(ByteBuffer compressed);
}
