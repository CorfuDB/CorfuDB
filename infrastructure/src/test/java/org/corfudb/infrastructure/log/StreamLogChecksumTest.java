package org.corfudb.infrastructure.log;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StreamLogChecksumTest {

    @Test
    public void testChecksumBytes() {
        assertEquals(506166820, StreamLogFiles.Checksum.getChecksum("corfuDb".getBytes()));
        assertEquals(-2036305216, StreamLogFiles.Checksum.getChecksum("test".getBytes()));
        assertEquals(-1819997757, StreamLogFiles.Checksum.getChecksum("checksum".getBytes()));
    }

    @Test
    public void testChecksumInt() {
        assertEquals(-698278101, StreamLogFiles.Checksum.getChecksum(42));
        assertEquals(2097792135, StreamLogFiles.Checksum.getChecksum(Integer.MAX_VALUE));
        assertEquals(-896438081, StreamLogFiles.Checksum.getChecksum(Integer.MIN_VALUE));
    }

}
