package org.corfudb.infrastructure.log;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class Checksum {

    private Checksum() {
        //prevent creating instances
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
