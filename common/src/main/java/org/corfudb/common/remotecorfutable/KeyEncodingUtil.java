package org.corfudb.common.remotecorfutable;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;

/**
 * This utility class aids with interacting with RocksDB keys.
 *
 * <p>Created by nvaishampayan517 on 7/26/21.
 */
public final class KeyEncodingUtil {
    //prevent instantiation
    private KeyEncodingUtil() {}

    /**
     * Extracts the encoded table key from a database key.
     * @param rocksDBKey Encoded database key.
     * @return Byte array representation of table key.
     */
    public static byte[] extractEncodedKey(byte[] rocksDBKey) {
        int keysize = rocksDBKey.length - 8;
        byte[] key = new byte[keysize];
        System.arraycopy(rocksDBKey, 0, key, 0, keysize);
        return key;
    }

    /**
     * Extracts the encoded table key from a database key.
     * @param rocksDBKey Encoded database key.
     * @return ByteString representation of table key.
     */
    public static ByteString extractEncodedKeyAsByteString(byte[] rocksDBKey) {
        int keysize = rocksDBKey.length - 8;
        return ByteString.copyFrom(rocksDBKey,0,keysize);
    }

    /**
     * Extracts the timestamp from a database key.
     * @param rocksDBkey Encoded database key.
     * @return Timestamp of the key.
     */
    public static long extractTimestamp(byte[] rocksDBkey) {
        int offset = rocksDBkey.length - 8;
        return Longs.fromBytes(rocksDBkey[offset], rocksDBkey[offset+1], rocksDBkey[offset+2], rocksDBkey[offset+3],
                rocksDBkey[offset+4], rocksDBkey[offset+5], rocksDBkey[offset+6], rocksDBkey[offset+7]);
    }

    public static byte[] composeKeyWithDifferentVersion(byte[] rocksDBKey, long desiredTimestamp) {
        byte[] newKey = new byte[rocksDBKey.length];
        int prefixSize = newKey.length - 8;
        System.arraycopy(rocksDBKey, 0, newKey, 0, prefixSize);
        System.arraycopy(Longs.toByteArray(desiredTimestamp), 0, newKey, prefixSize, 8);
        return newKey;
    }
}
