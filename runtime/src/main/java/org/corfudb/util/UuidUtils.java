package org.corfudb.util;

import com.google.common.io.BaseEncoding;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.annotation.Nonnull;

/** A collection of utilities to manage and manipulate UUIDs. */
public class UuidUtils {

    /** Generate a base64 URL-safe string from a given UUID.
     *
     * @param uuid      The UUID to convert.
     * @return          A base64 URL-safe string for the UUID.
     */
    public static String asBase64(@Nonnull UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return BaseEncoding.base64Url().omitPadding().encode(bb.array());
    }

    /** Generate a UUID from a base64 URL-safe string.
     *
     * @param uuidString    The base64 URL-safe string to convert.
     *
     * @return                            A UUID from the string.
     * @throws IllegalArgumentException   If decoding fails due to a malformed string.
     */
    public static UUID fromBase64(@Nonnull String uuidString) {
        ByteBuffer bb = ByteBuffer.wrap(BaseEncoding.base64Url().decode(uuidString));
        if (bb.remaining() < 16) {
            throw new IllegalArgumentException("Input too short: must be 16 bytes");
        } else if (bb.remaining() > 16) {
            throw new IllegalArgumentException("Input too long: must be 16 bytes");
        }
        return new UUID(bb.getLong(), bb.getLong());
    }
}
