package org.corfudb.infrastructure.remotecorfutable.utils;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.DATABASE_CHARSET;

/**
 * This utility class aids with constructing a RocksDB store key from a RemoteCorfuTable key and a timestamp, and
 * recovering those values from an existing key. All RemoteCorfuTable -> RocksDB keys must be created by this class
 * to prevent any format differences.
 *
 * <p>Created by nvaishampayan517 on 7/26/21.
 */
public final class KeyEncodingUtil {
    //prevent instantiation
    private KeyEncodingUtil() {}

    /**
     * Creates RocksDB key from RemoteCorfuTable key and timestamp.
     * @param encodedRemoteCorfuTableKey Encoded key from client-side table.
     * @param timestamp Versioning timestamp of request.
     * @return Key for storage in RocksDB.
     */
    public static byte[] constructDatabaseKey(byte[] encodedRemoteCorfuTableKey, long timestamp) {
        return Bytes.concat(encodedRemoteCorfuTableKey, Longs.toByteArray(timestamp));
    }

    public static boolean validateDatabaseKey(byte[] databaseKey) {
        return databaseKey != null && databaseKey.length >= 8;
    }

    /**
     * Extracts original encoded key and timestamp from database key.
     * @param rocksDBKey Encoded database key.
     * @return VersionedKey instance containing original encoded key, key size, and timestamp.
     */
    public static VersionedKey extractVersionedKey(byte[] rocksDBKey) {
        int keySize = rocksDBKey.length - 8;
        byte[] key = new byte[keySize];
        byte[] version = new byte[8];
        System.arraycopy(rocksDBKey, 0, key, 0, keySize);
        System.arraycopy(rocksDBKey, keySize, version, 0, 8);
        return new VersionedKey(key, Longs.fromByteArray(version));
    }

    /**
     * Extracts timestamp as byte array from database key - use to avoid converting the byte array
     * timestamp stored in the key to a long and converting back to a byte array.
     * @param rocksDBKey Encoded database key.
     * @return Byte array representation of timestamp associated with key.
     */
    public static byte[] extractTimestampAsByteArray(byte[] rocksDBKey) {
        byte[] timestampBytes = new byte[8];
        System.arraycopy(rocksDBKey, rocksDBKey.length-8, timestampBytes, 0, 8);
        return timestampBytes;
    }

    /**
     * Extracts the timestamp from a database key.
     * @param rocksDBKey Encoded database key.
     * @return Long representation of timestamp associated with key.
     */
    public static long extractTimestamp(byte[] rocksDBKey) {
        byte[] timestampBytes = new byte[8];
        System.arraycopy(rocksDBKey, rocksDBKey.length-8, timestampBytes, 0, 8);
        return Longs.fromByteArray(timestampBytes);
    }

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
     * This class is used to extract information from an encoded key. It can only be instantiated from
     * KeyEncodingUtil#extractEncodedKey.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class VersionedKey {
        private final byte[] encodedRemoteCorfuTableKey;
        private final long timestamp;

        @Override
        public String toString() {
            return (new String(encodedRemoteCorfuTableKey, DATABASE_CHARSET) + timestamp);
        }
    }
}
