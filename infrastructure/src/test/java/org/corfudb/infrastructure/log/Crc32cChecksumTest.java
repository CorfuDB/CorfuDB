package org.corfudb.infrastructure.log;

import org.junit.Test;

import static org.corfudb.infrastructure.utils.Crc32c.getChecksum;
import static org.junit.Assert.assertEquals;

public class Crc32cChecksumTest {

    @Test
    public void testChecksumBytes() {
        assertEquals(506166820, getChecksum("corfuDb".getBytes()));
        assertEquals(-2036305216, getChecksum("test".getBytes()));
        assertEquals(-1819997757, getChecksum("checksum".getBytes()));
    }

    @Test
    public void testChecksumInt() {
        assertEquals(-698278101, getChecksum(42));
        assertEquals(2097792135, getChecksum(Integer.MAX_VALUE));
        assertEquals(-896438081, getChecksum(Integer.MIN_VALUE));
    }

}
