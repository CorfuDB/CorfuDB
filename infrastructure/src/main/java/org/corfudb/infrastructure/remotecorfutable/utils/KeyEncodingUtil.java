package org.corfudb.infrastructure.remotecorfutable.utils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.nio.ByteBuffer;

/**
 * This utility class aids with constructing a RocksDB store key from a RemoteCorfuTable key and a timestamp, and
 * recovering those values from an existing key. All RemoteCorfuTable -> RocksDB keys must be created by this class
 * to prevent any format differences.
 *
 * <p>Created by nvaishampayan517 on 7/26/21.
 */
public final class KeyEncodingUtil {
    //TODO: explore reusing byte buffer -> maybe have pool of ByteBuffers?
    //TODO: possibly remove byte buffers entirely -> work only with loops and byte[]
    //prevent instantiation
    private KeyEncodingUtil() {}

    /**
     * Creates RocksDB key from RemoteCorfuTable key and timestamp.
     * @param encodedRemoteCorfuTableKey Encoded key from client-side table.
     * @param encodedKeySize Size of encoded key.
     * @param timestamp Versioning timestamp of request.
     * @return Key for storage in RocksDB.
     */
    public static byte[] constructDatabaseKey(byte[] encodedRemoteCorfuTableKey, int encodedKeySize, long timestamp) {
        int bufferSize = Integer.BYTES + encodedKeySize + Long.BYTES;
        ByteBuffer keyBuffer = ByteBuffer.allocate(bufferSize);
        keyBuffer.putInt(encodedKeySize).put(encodedRemoteCorfuTableKey, 0, encodedKeySize).putLong(timestamp);
        return keyBuffer.array();
    }

    /**
     * Extracts original encoded key and timestamp from database key.
     * @param rocksDBKey Encoded database key.
     * @return VersionedKey instance containing original encoded key, key size, and timestamp.
     */
    public static VersionedKey extractEncodedKey(byte[] rocksDBKey) {
        ByteBuffer keyBuffer = ByteBuffer.wrap(rocksDBKey);
        int keySize = keyBuffer.getInt(0);
        byte[] key = new byte[keySize];
        keyBuffer.get(key, 4, keySize);
        long timestamp = keyBuffer.getLong(4 + keySize);
        return new VersionedKey(key, keySize, timestamp);
    }

    /**
     * This class is used to extract information from an encoded key. It can only be instantiated from
     * KeyEncodingUtil#extractEncodedKey.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class VersionedKey {
        private byte[] encodedRemoteCorfuTableKey;
        private int encodedKeySize;
        private long timestamp;
    }
}
