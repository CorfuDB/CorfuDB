package org.corfudb.common.compression;

import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class ZSTDCompression implements Codec {

    private static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private static ZSTDCompression INSTANCE = new ZSTDCompression();

    public static ZSTDCompression getInstance() {
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
        final int maxCompressedLength = (int) Zstd.compressBound(decompressedLength);

        byte[] compressed = new byte[maxCompressedLength + Integer.BYTES];
        ByteBuffer wrappedBuf = ByteBuffer.wrap(compressed);
        wrappedBuf.putInt(decompressedLength);

        long compressedLen = Zstd.compressByteArray(compressed, Integer.BYTES, maxCompressedLength,
                uncompressed.array(), uncompressed.position(), uncompressed.remaining(),
                DEFAULT_COMPRESSION_LEVEL);

        if (Zstd.isError(compressedLen)) {
            throw new IllegalStateException("Compression failed with error code " + compressedLen);
        }

        wrappedBuf.position((int) compressedLen + Integer.BYTES);
        wrappedBuf.flip();
        return  wrappedBuf;
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
        byte[] restored = new byte[decompressedSize];
        ByteBuffer wrappedBuf = ByteBuffer.wrap(restored);

        long restoredBytes = Zstd.decompressByteArray(restored, 0, restored.length,
                compressed.array(), Integer.BYTES, compressed.remaining());

        if (Zstd.isError(restoredBytes)) {
            throw new IllegalStateException("Decompression failed with error code " + restoredBytes);
        }

        wrappedBuf.position((int) restoredBytes);
        wrappedBuf.flip();
        return wrappedBuf;
    }
}
