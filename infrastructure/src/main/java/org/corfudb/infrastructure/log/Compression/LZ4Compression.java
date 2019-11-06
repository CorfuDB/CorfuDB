package org.corfudb.infrastructure.log.Compression;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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

    @Override
    public ByteBuffer compress(ByteBuffer uncompressed) {
        checkNotNull(uncompressed);
        checkArgument(uncompressed.hasRemaining());

        final int decompressedLength = uncompressed.remaining();

        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);

        ByteBuffer compressed = ByteBuffer.allocate(maxCompressedLength + Integer.BYTES);

        int compressedLen = compressor.compress(uncompressed, 0, decompressedLength, compressed,
                Integer.BYTES, maxCompressedLength);
        compressed.putInt(0, decompressedLength);
        compressed.position(compressedLen + Integer.BYTES);
        compressed.flip();
        return compressed;
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        checkNotNull(compressed);
        checkArgument(compressed.remaining() > Integer.BYTES);

        int decompressedSize = compressed.getInt();

        ByteBuffer restored = ByteBuffer.allocate(decompressedSize);

        int restoredBytes = decompressor.decompress(compressed, Integer.BYTES, restored, 0, decompressedSize);

        if (restoredBytes != decompressedSize) {
            // Why is this always off-by one?
        }

        return restored;
    }
}
