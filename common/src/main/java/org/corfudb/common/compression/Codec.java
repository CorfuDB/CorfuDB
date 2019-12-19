package org.corfudb.common.compression;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

        NONE(0, NoCompression::getInstance),
        LZ4(1, LZ4Compression::getInstance),
        ZSTD(2, ZSTDCompression::getInstance);

        /**
         * The unique id of the code that will be used
         * during serialization/deserialization
         */
        final int id;

        private Supplier<Codec> func;

        public static Map<Integer, Type> typeMap = Arrays.stream(Codec.Type.values())
                .collect(Collectors.toMap(Codec.Type::getId, Function.identity()));

        Type(int id, Supplier<Codec> func) {
            this.id = id;
            this.func = func;
        }

        public Codec getInstance() {
            return func.get();
        }

        public int getId() {
            return id;
        }
    }

    static Codec.Type getCodecTypeById(int id) {
        Codec.Type type = Type.typeMap.get(id);
        Objects.requireNonNull(type, "Unknown codec id " + id);
        return type;
    }

    /**
     * Returns the compressed ByteBuffer as a ByteBuffer with the size of
     * the uncompressed buffer as the first 4-bytes only if input buffer has
     * bytes.
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
