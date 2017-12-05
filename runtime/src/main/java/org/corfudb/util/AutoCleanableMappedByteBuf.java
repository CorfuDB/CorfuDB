package org.corfudb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.MappedByteBuffer;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/** {@link this} wraps around a {@link MappedByteBuffer}, implementing the
 *  {@link java.lang.AutoCloseable} interface which enables a {@link MappedByteBuffer} to be
 *  automatically cleaned via a try-resources block.
 *
 *  <p>Once the buffer is closed it should no longer be used.
 */
@Slf4j
public class AutoCleanableMappedByteBuf implements AutoCloseable {

    /** The {@link java.nio.MappedByteBuffer} being wrapped. */
    @Getter
    private final MappedByteBuffer buffer;

    /** A {@link ByteBuf} which wraps the {@link MappedByteBuffer}. */
    @Getter
    private final ByteBuf byteBuf;

    /** Construct a new {@link AutoCleanableMappedByteBuf}.
     *
     * @param buffer    The {@link MappedByteBuffer} to wrap.
     */
    public AutoCleanableMappedByteBuf(@Nonnull MappedByteBuffer buffer) {
        this.buffer = buffer;
        this.byteBuf = Unpooled.wrappedBuffer(buffer);
    }

    /** {@inheritDoc}
     *
     * @throws UnsupportedOperationException    If the buffer could not be auto-cleaned.
     */
    @Override
    public void close() {
        try {
            Cleaner c = ((DirectBuffer) buffer).cleaner();
            if (c != null) {
                 c.clean();
            }
        } catch (Exception ex) {
            throw new UnsupportedOperationException("Failed to autoclean buffer", ex);
        }
    }
}
