package org.corfudb.utils.remotecorfutable;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import lombok.NonNull;

/**
 * The RemoteCorfuTableVersionedKey provides an interface for creating and reading
 * rocksDB keys for storage in the server.
 *
 * <p>Created by nvaishampayan517 on 8/5/21.
 */
public class RemoteCorfuTableVersionedKey {
    private ByteString encodedVersionedKey;
    private ByteString encodedRemoteCorfuTableKey;
    private Long timestamp;

    /**
     * Constructs a RemoteCorfuTableVersionedKey from key and timestamp - used to
     * create a key for storage in the database.
     * @param encodedRemoteCorfuTableKey Serialized Corfu Table key from client.
     * @param timestamp Timestamp of request.
     */
    public RemoteCorfuTableVersionedKey(@NonNull ByteString encodedRemoteCorfuTableKey, long timestamp) {
        this.encodedRemoteCorfuTableKey = encodedRemoteCorfuTableKey;
        this.timestamp = timestamp;
        this.encodedVersionedKey = null;
    }

    /**
     * Constructs a RemoteCorfuTableVersionedKey from rocksDB key - used to
     * read information from the key.
     * @param encodedVersionedKey RocksDB key
     */
    public RemoteCorfuTableVersionedKey(byte[] encodedVersionedKey) {
        if (encodedVersionedKey == null || encodedVersionedKey.length < 8) {
            throw new RuntimeException("Invalid database key parameter");
        }
        this.encodedVersionedKey = ByteString.copyFrom(encodedVersionedKey);
        this.encodedRemoteCorfuTableKey = null;
        this.timestamp = null;
    }

    /**
     * Lazily retrieves ByteString representation of rocksDB key.
     * @return ByteString representation of rocksDB key
     */
    public ByteString getEncodedVersionedKey() {
        if (encodedVersionedKey == null) {
            //if encodedVersionedKey is null, timestamp and key must not be null
            constructVersionedKey();
        }
        return encodedVersionedKey;
    }

    /**
     * Lazily retrieves timestamp as byte array. Used for storing
     * timestamp metadata in database.
     * @return byte[] representation of key timestamp
     */
    public byte[] getTimestampAsByteArray() {
        if (timestamp == null) {
            //if timestamp is null, versioned key must not be null
            byte[] timestampBytes = new byte[8];
            encodedVersionedKey.copyTo(timestampBytes,
                    encodedVersionedKey.size()-8, 0, 8);
            timestamp = Longs.fromByteArray(timestampBytes);
            return timestampBytes;
        } else {
            return Longs.toByteArray(timestamp);
        }
    }

    /**
     * Lazily retrieves timestamp as long.
     * @return long representation of key timestamp
     */
    public long getTimestamp() {
        if (timestamp == null) {
            //if timestamp is null, versioned key must not be null
            byte[] timestampBytes = new byte[8];
            encodedVersionedKey.copyTo(timestampBytes,
                    encodedVersionedKey.size()-8, 0, 8);
            timestamp = Longs.fromByteArray(timestampBytes);
        }
        return timestamp;
    }

    /**
     * Lazily retrieves client key as ByteString.
     * @return ByteString representation of client key
     */
    public ByteString getEncodedKey() {
        if (encodedRemoteCorfuTableKey == null) {
            //if key is null, versioned key must not be null
            int keysize = encodedVersionedKey.size() - 8;
            encodedRemoteCorfuTableKey = encodedVersionedKey.substring(0, keysize);
        }
        return encodedRemoteCorfuTableKey;
    }

    /**
     * Retrieves the length of the versionedkey.
     * @return length of the versioned key
     */
    public int size() {
        if (encodedVersionedKey == null) {
            constructVersionedKey();
        }

        return encodedVersionedKey.size();
    }

    private void constructVersionedKey() {
        encodedVersionedKey = encodedRemoteCorfuTableKey.concat(ByteString.copyFrom(Longs.toByteArray(timestamp)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteCorfuTableVersionedKey that = (RemoteCorfuTableVersionedKey) o;
        return getEncodedVersionedKey().equals(that.getEncodedVersionedKey());
    }

    @Override
    public int hashCode() {
        return getEncodedVersionedKey().hashCode();
    }

    @Override
    public String toString() {
        return (encodedRemoteCorfuTableKey.toString() + timestamp);
    }
}
