package org.corfudb.infrastructure.utils;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public final class Crc32c {

    private Crc32c() {

    }

    /**
     * Returns checksum used for log.
     *
     * @param bytes data over which to compute the checksum
     * @return checksum of bytes
     */
    public static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();
        for (byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    public static int getChecksum(int num) {
        Hasher hasher = Hashing.crc32c().newHasher();
        return hasher.putInt(num).hash().asInt();
    }
}
