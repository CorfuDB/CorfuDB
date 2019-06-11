package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.corfudb.format.Types;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by Maithem on 6/11/19.
 */
public class Lz4Compressor implements Compressor {

    private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
    private static final LZ4Compressor compressor = lz4Factory.fastCompressor();
    private static final LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

    @Override
    public Types.CompressionType getType() {
        return Types.CompressionType.LZ4;
    }

    @Override
    public ByteBuffer encode(ByteBuffer buffer) {
        int uncompressedLength = buffer.remaining();
        int maxLength = compressor.maxCompressedLength(uncompressedLength);

        ByteBuffer sourceNio = ByteBuffer.allocate(buffer.remaining());

        ByteBuf target = PooledByteBufAllocator.DEFAULT.buffer(maxLength, maxLength);
        ByteBuffer targetNio = target.nioBuffer(0, maxLength);

        int compressedLength = compressor.compress(sourceNio, 0, uncompressedLength, targetNio, 0, maxLength);
        target.writerIndex(compressedLength);
        return target.nioBuffer();
    }

    @Override
    public ByteBuffer decode(ByteBuffer encoded) {
        ByteBuffer uncompressed = PooledByteBufAllocator.DEFAULT.buffer(encoded.remaining(), encoded.remaining()).nioBuffer();
        ByteBuffer uncompressedNio = ByteBuffer.allocate(uncompressed.remaining());

        decompressor.decompress(uncompressedNio, uncompressedNio.position(), uncompressedNio, uncompressedNio.position(),
                uncompressedNio.remaining());
        return uncompressed;
    }
}
