package org.corfudb.common.compression;

import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ZSTDCompression implements Codec {

    private static final int DEFAULT_COMPRESSION_LEVEL = 3;
    private static final int EXPECTED_COMPRESSION_RATIO = 4;
    private static final int EXPECTED_MAX_PAYLOAD_SIZE = 100_000_000;

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

        final int uncompressedLength = uncompressed.remaining();
        MicroMeterUtils.measure(uncompressedLength, "logdata.uncompressed.size");
        final int maxCompressedLength = (int) Zstd.compressBound(uncompressedLength);

        byte[] compressed = new byte[maxCompressedLength + Integer.BYTES];
        ByteBuffer wrappedBuf = ByteBuffer.wrap(compressed);
        wrappedBuf.putInt(uncompressedLength);

        long compressedLen = Zstd.compressByteArray(compressed, Integer.BYTES, maxCompressedLength,
                uncompressed.array(), uncompressed.position(), uncompressed.remaining(),
                DEFAULT_COMPRESSION_LEVEL);

        if (Zstd.isError(compressedLen)) {
            throw new IllegalStateException("Compression failed with error code " + compressedLen);
        }

        double compressionRatio = (double) uncompressedLength/compressedLen;
        MicroMeterUtils.measure(compressionRatio, "logdata.compression.ratio");
        if (compressionRatio > EXPECTED_COMPRESSION_RATIO) {
            log.debug("High compression ratio: {}", compressionRatio);
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
        if (decompressedSize > EXPECTED_MAX_PAYLOAD_SIZE) {
            log.warn("Decompressed size of payload in bytes: {}", decompressedSize);
        }
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
