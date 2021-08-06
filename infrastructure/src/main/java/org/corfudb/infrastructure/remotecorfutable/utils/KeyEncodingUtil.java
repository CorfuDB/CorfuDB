package org.corfudb.infrastructure.remotecorfutable.utils;

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
}
