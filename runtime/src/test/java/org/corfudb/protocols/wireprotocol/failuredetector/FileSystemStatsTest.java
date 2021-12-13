package org.corfudb.protocols.wireprotocol.failuredetector;

import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.ResourceQuotaStats;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemStatsTest {

    @Test
    public void testExceeded() {
        final long limit = 100;
        final long used = 200;
        ResourceQuotaStats stats = new ResourceQuotaStats(limit, used);
        assertTrue(stats.isExceeded());
    }

    @Test
    public void testAvailable() {
        final long limit = 100;
        final long used = 20;
        final int available = 80;

        ResourceQuotaStats stats = new ResourceQuotaStats(limit, used);
        assertEquals(available, stats.available());
    }

    @Test
    public void testZeroAvailableSpace() {
        final long limit = 100;
        final long used = 200;
        final int available = 0;

        ResourceQuotaStats stats = new ResourceQuotaStats(limit, used);
        assertEquals(available, stats.available());
    }
}