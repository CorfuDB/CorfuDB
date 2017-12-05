package org.corfudb.util;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import javax.annotation.Nonnull;

/** This class provides an {@link ByteBuf} view over a memory-mapped file. It automatically
 *  allocates a memory mapped buffer over a {@link FileChannel} given at construction time,
 *  and creates new mappings as necessary during stream traversal. The mapping(s) are released
 *  when the stream is closed and during stream traversal when they are no longer necessary.
 *
 */
public class MappedByteBufInputStream extends InputStream implements AutoCloseable {

    /** The underlying {@link FileChannel} to map. */
    private final FileChannel fc;

    /** The position to start the stream from. */
    private final long startPosition;

    /** The total length of the stream. */
    private final long totalLength;

    /** The current mapped buffer. Updated as the stream is traversed. */
    private AutoCleanableMappedByteBuf buffer;

    /** The current position in the stream. Updated as the stream is traversed. */
    private long currentPosition;

    /** Generate a new {@link MappedByteBufInputStream} over the entire file.
     *
     * @param fc                The FileChannel to map the stream over.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc)
            throws IOException {
        this(fc, fc.size());
    }

    /** Generate a new {@link MappedByteBufInputStream} over a file length.
     *
     * @param fc                The FileChannel to map the stream over.
     * @param length            The length of the file to map.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc, long length)
            throws IOException {
        this(fc, 0, length);
    }

    /** Generate a new {@link MappedByteBufInputStream} over a file length and start position.
     *
     * @param fc                The FileChannel to map the stream over.
     * @param position          The position to start at.
     * @param length            The length of the file to map.
     * @throws IOException      If the stream cannot be mapped.
     */
    public MappedByteBufInputStream(@Nonnull FileChannel fc, long position, long length)
            throws IOException {
        this.fc = fc;
        this.startPosition = position;
        this.totalLength = length;
        reset();
    }

    /** Update {@code buffer} with the given position and length.
     *
     * @param position          The position the buffer should be mapped to in the file.
     * @param length            The length of the mapping, which must be < 2GB.
     * @throws IOException      If an I/O exception was encountered during mapping.
     */
    private void getNewBuffer(long position, int length) throws IOException {
        if (buffer != null) {
            buffer.close();
        }
        buffer = new AutoCleanableMappedByteBuf(fc.map(MapMode.READ_ONLY, position, length));
    }

    /** Ensure that the current buffer has {@code size} bytes available to read, possibly updating
     *  {@code buffer} if necessary.
     *
     *  <p>If {@code size} bytes are not available to read, the current {@code buffer} is guaranteed
     *  to contain all remaining bytes in the stream.
     *
     * @param size               The number of bytes to ensure are readable.
     * @return                   True, if the the buffer contains all the data, False otherwise.
     */
    private boolean ensureReadable(int size)
            throws IOException {
        if (size > availableInBuffer()) {
            // Not enough data in the current buffer. Remap the buffer.
            getNewBuffer(currentPosition, (int) Math.min(Integer.MAX_VALUE, availableBytes()));
        }

        if (size > availableBytes()) {
            currentPosition = startPosition + totalLength - 1;
            return false;
        }

        currentPosition += size;
        return true;
    }

    /** Return a limited version of this {@link MappedByteBufInputStream} which only returns
     * {@code limit} bytes.
     *
     * @param limit             The number of bytes to limit to.
     * @return                  An {@link InputStream} which only permits reading up to the
     *                          given number of bytes.
     */
    public InputStream limited(int limit) {
        return ByteStreams.limit(this, limit);
    }

    /** Get the position of the reader into this stream.
     *
     * @return                  The position of this stream, in bytes.
     */
    public long position() {
        return currentPosition;
    }

    /** Get how many bytes are available to be consumed via read calls.
     *
     * <p>Due to limitations of {@link InputStream}, this method is limited to
     * {@link Integer.MAX_VALUE}. To retrieve the actual number of bytes, call
     * {@link this#availableBytes()}.
     *
     * @return                  The number of bytes available to be consumed.
     */
    public int available() {
        return (int) Math.min(Integer.MAX_VALUE, availableBytes());
    }

    /** Get how many bytes are available to be consumed via read calls.
     *
     * @return                  The number of bytes available to be consumed.
     */
    public long availableBytes() {
        return totalLength - (currentPosition - startPosition);
    }

    /** Get how many bytes are available in the current {@link buffer}.
     *
     * @return                  The number of bytes available in the current buffer.
     */
    private int availableInBuffer() {
        if (buffer == null) {
            return 0;
        }
        return buffer.getByteBuf().writerIndex() - buffer.getByteBuf().readerIndex();
    }

    /** Skip the given number of bytes.
     *
     * @param bytes             The number of bytes to skip.
     */
    public long skipBytes(long bytes)
            throws IOException {
        if (bytes == 0) {
            return 0;
        } else if (bytes < 0) {
            throw new IllegalArgumentException("Must skip positive bytes!");
        }

        long skipped = Math.min(bytes, availableBytes());
        currentPosition += skipped;

        if (availableInBuffer() >= skipped) {
            // All the skipped bytes are available in the buffer, so just move
            // the buffer index
            buffer.getByteBuf().readerIndex(buffer.getByteBuf().readerIndex() + (int) bytes);
        } else {
            // Otherwise, we need to get a new buffer
            getNewBuffer(currentPosition, (int) Math.min(Integer.MAX_VALUE, availableBytes()));
        }
        return skipped;
    }

    @Override
    public int read() throws IOException {
        if (!ensureReadable(1)) {
            return -1;
        }
        return buffer.getByteBuf().readByte() & 255;
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
        if (!ensureReadable(len)) {
            // check if anything is available in the current buffer
            int available = availableInBuffer();
            if (available > 0) {
                buffer.getByteBuf().readBytes(b, off, available);
                return available;
            } else {
                // Nothing is available.
                return -1;
            }
        } else {
            buffer.getByteBuf().readBytes(b, off, len);
            return len;
        }
    }

    @Override
    public void reset() throws IOException {
        int thisSegmentLength = (int) Math.min(Integer.MAX_VALUE, totalLength);
        getNewBuffer(startPosition, thisSegmentLength);
        currentPosition = startPosition;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long skip(long bytes) throws IOException {
        return skipBytes(bytes);
    }

    /** Read a {@link short}
     *
     * @return  The next {@link short}.
     */
    public short readShort() throws IOException {
        if (!ensureReadable(Short.BYTES)) {
            throw new IndexOutOfBoundsException();
        }
        return buffer.getByteBuf().readShort();
    }

    /** {@inheritDoc}
     *
     *  <Pp>Also closes the underlying {@link AutoCleanableMappedByteBuf}.
     */
    @Override
    public void close() {
        buffer.close();
        buffer = null;
    }
}
