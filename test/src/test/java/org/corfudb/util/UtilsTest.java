package org.corfudb.util;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class UtilsTest {

    /**
     * Even though we are using a library implementation to compute
     * checksums, this is mostly a sanity check.
     */
    @Test
    public void computeChecksumTest() {

        Random rand = new Random();
        int numTest = 1000;
        int max_num_bytes = 4000;
        int min_num_bytes = 400;

        for (int x = 0; x < numTest; x++){
            int numBytes = rand.nextInt(max_num_bytes - min_num_bytes) + min_num_bytes;
            byte[] src = new byte[numBytes];

            // Generate random data and compute checksum
            rand.nextBytes(src);

            Checksum checksum = Utils.getChecksum(src);

            // Change 5 bytes randomly of bytes
            for (int a = 0; a < 5; a++){
                int index = rand.nextInt(src.length - 1);
                src[index] = 0;
            }

            Checksum checksum2 = Utils.getChecksum(src);

            assertThat(checksum).isNotEqualTo(checksum2);
        }

    }

}
