package org.corfudb.common.compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 * An implementation of an LZ4 codec.
 *
 * Created by Maithem on 11/6/19.
 */
public class LZ4Compression implements Codec {
    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    private static LZ4Compression INSTANCE = new LZ4Compression();

    public LZ4Compression() {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        compressor = factory.fastCompressor();
        decompressor = factory.fastDecompressor();
    }

    public static LZ4Compression getInstance() {
        return INSTANCE;
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        Objects.requireNonNull(uncompressed);
        checkArgument(uncompressed.hasRemaining());

        final int decompressedLength = uncompressed.remaining();

        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);

        ByteBuffer compressed = ByteBuffer.allocate(maxCompressedLength + Integer.BYTES);
        compressed.putInt(decompressedLength);
        int compressedLen = compressor.compress(uncompressed, 0, decompressedLength, compressed,
                compressed.position(), maxCompressedLength);
        compressed.position(compressedLen + Integer.BYTES);
        compressed.flip();
        return compressed;
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        Objects.requireNonNull(compressed);
        checkArgument(compressed.remaining() > Integer.BYTES);

        int decompressedSize = compressed.getInt();

        ByteBuffer restored = ByteBuffer.allocate(decompressedSize);

        decompressor.decompress(compressed, Integer.BYTES, restored, 0, decompressedSize);

        return restored;
    }
}
