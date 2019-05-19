package org.corfudb.platform.core;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link Value} that represents an immutable sequence of bytes.
 * <p>
 * Note regarding interface prefix naming conventions:
 * <ul>
 * <li>as* {@literal =>} Some kind of cast or low-cost conversion / wrapping, *if possible*, or
 * degenerates to copy-conversion if not possible.
 * </li>
 * <li>to* {@literal =>} Copy-conversion (buffer-transfer).</li>
 * </ul>
 */
public class ByteString implements Value, Serializable, Comparable<ByteString> {

    /**
     * A static instance of {@link ByteString} denoting an empty data content container.
     */
    private static final ByteString EMPTY = new ByteString(new byte[0]);

    private final byte[] data;
    private final int offset;
    private final int length;
    private transient int hashCode;

    protected ByteString(byte[] data) {
        this(data, 0, data.length);
    }

    protected ByteString(byte[] data, int offset, int length) {
        Objects.requireNonNull(data);

        this.data = data;
        this.offset = offset;
        this.length = Math.min(data.length - offset, length);
    }

    /**
     * Retrieves a {@code byte[]} containing the content within this {@link ByteString}.
     * <p>
     * Unlike {@link #toBytes()}, the {@code byte[]} returned from this call may be referencing the
     * raw buffer. In this case, the caller would end up sharing memory with this {@link ByteString}
     * instance. Thus, any mutation performed on this data may potentially violate the immutability
     * contract.
     *
     * @return raw internal {@code byte[]} data.
     */
    @Override
    public byte[] asBytes() {
        return (offset == 0 && length == data.length) ? data : toBytes();
    }

    /**
     * Retrieves a {@code byte[]} containing a copy of the content within this {@link ByteString}.
     *
     * @return a clone of the internal {@code byte[]} data.
     */
    public byte[] toBytes() {
        if (offset == 0 && length == data.length) {
            return data.clone();
        } else {
            // Return a sub-region-only copy.
            byte[] copy = new byte[length];
            System.arraycopy(data, offset, copy, 0, length);
            return copy;
        }
    }

    /**
     * Creates a view of the bytes within this {@link ByteString} as a read-only {@link ByteBuffer}.
     *
     * @return a read-only {@link ByteBuffer} instance.
     */
    public ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(data, offset, length).asReadOnlyBuffer();
    }

    /**
     * Returns the size of the data content contained in this {@link ByteString} instance.
     *
     * @return the number of bytes of the data content as an {@code int}.
     */
    public int size() {
        return length;
    }

    /**
     * Creates a slice of this {@link ByteString} that represents a sub-region of the original data.
     *
     * @param offset starting offset of the slice.
     * @param length length of the slice.
     * @return a new instance of {@link ByteString} representing the sliced data.
     */
    public Optional<ByteString> slice(int offset, int length) {
        if (validateSliceRange(this, offset, length)) {
            return Optional.of(length == 0 ?
                    empty() :
                    new ByteString(data, this.offset + offset, length));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ByteString)) {
            return false;
        }

        ByteString that = (ByteString) o;
        if (this.length != that.length) {
            return false;
        }

        if (offset == that.offset) {
            return Arrays.equals(data, that.data);
        } else {
            // Note: JDK 9 supports vectorized array mismatch offset-based comparisons. It should be
            // used instead when JDK 9 is used.
            if (length < 7) {
                for (int i = 0; i < length; i++) {
                    if (data[i] != that.data[i])
                        return false;
                }
                return true;
            } else {
                return Arrays.equals(asBytes(), that.asBytes());
            }
        }
    }

    @Override
    public int hashCode() {
        // If hashCode is uninitialized, then compute it and return the value (race doesn't matter).
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(asBytes());
        }

        return hashCode;
    }

    @Override
    public int compareTo(ByteString other) {
        int size1 = length;
        int size2 = other.length;
        for (int i = 0, size = Math.min(size1, size2); i < size; i++) {
            int byte1 = data[offset + i] & 0xff;
            int byte2 = other.data[other.offset + i] & 0xff;
            if (byte1 != byte2) {
                return byte1 - byte2;
            }
        }

        return size1 - size2;
    }

    @Override
    public String toString() {
        return "ByteString{offset=" + offset + ", length=" + length + '}';
    }

    /**
     * Validates the input {@link ByteString} may contain a slice ranging from input {@code offset}
     * and {@code length}.
     *
     * @param instance instance to check for slice range.
     * @param offset   starting offset of the slice within the {@code instance}.
     * @param length   length of the slice.
     * @return {@code true} if {@code instance} may contain the slice, {@code false} otherwise.
     */
    private static boolean validateSliceRange(ByteString instance, int offset, int length) {
        return (offset >= 0 &&
                length >= 0 &&
                (instance.offset + offset + length) <= instance.length);
    }

    /**
     * Create an empty {@link ByteString}.
     *
     * @return an empty {@link ByteString} instance with zero-length for data content.
     */
    public static ByteString empty() {
        return EMPTY;
    }

    /**
     * Create a new {@link ByteString} containing a clone of the bytes of {@code data}.
     *
     * @param data data content.
     * @return an instance of {@link ByteString} with internal offset set to 0, and length to the
     * length of the input {@code data} byte array.
     */
    public static ByteString of(byte... data) {
        if (data == null) {
            return empty();
        }

        return new ByteString(data.clone());
    }

    /**
     * Create a new {@link ByteString} containing a clone of the bytes of {@code data}.
     *
     * @param data data content.
     * @return an instance of {@link ByteString} with internal offset set to 0, and length to the
     * length of the data read from {@code data} (i.e. {@code data.remaining()}).
     */
    public static ByteString of(ByteBuffer data) {
        if (data == null) {
            return empty();
        }

        // Return a copy of the data in the ByteBuffer.
        byte[] clone = new byte[data.remaining()];
        data.get(clone);
        return ByteString.of(clone);
    }

    /**
     * Create a new {@link ByteString} containing a copy of {@code length} bytes of {@code data}
     * starting at {@code offset}.
     *
     * @param data   data content.
     * @param offset offset to data content to start including from.
     * @param length length of data content to be included as byte-string's content.
     * @return an instance of {@link ByteString} with internal offset set to 0, and content
     * length to {@code length - offset}.
     */
    public static ByteString of(byte[] data, int offset, int length) {
        if (data == null || length == 0) {
            return empty();
        }

        byte[] copy = new byte[length];
        System.arraycopy(data, offset, copy, 0, length);
        return new ByteString(copy);
    }

    public static ByteString of(String data, Charset charset) {
        return of(data.getBytes(charset));
    }

    /**
     * Converts a {@link ByteString} to {@link ByteString} encoded in base64 encoding.
     *
     * @param instance instance to convert from.
     * @return base64 encoding of the data as a {@link ByteString}.
     */
    public static ByteString encodeBase64(ByteString instance) {
        return new ByteString(Base64.getEncoder().encode(instance.asBytes()));
    }

    /**
     * Converts a encoded base64 {@link ByteString} to a decoded {@link ByteString}.
     *
     * @param instance instance to convert from.
     * @return decoded content of the base64-encoded string as a {@link ByteString}.
     */
    public static ByteString decodeBase64(ByteString instance) {
        return new ByteString(Base64.getDecoder().decode(instance.asBytes()));
    }

    /**
     * Converts a encoded UTF-8 {@link ByteString} to a decoded {@link String}.
     *
     * @param instance instance to convert from.
     * @return UTF-8 decoded as a {@link String} from the data content.
     */
    public static String utf8(ByteString instance) {
        return new String(instance.data, instance.offset, instance.length, StandardCharsets.UTF_8);
    }
}
