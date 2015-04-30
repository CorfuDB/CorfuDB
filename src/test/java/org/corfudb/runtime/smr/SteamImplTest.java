package org.corfudb.runtime.smr;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SteamImplTest {
    @Test
    public void isSequence()
    {
        MemoryStreamImpl.MemoryLog msl = new MemoryStreamImpl.MemoryLog();
        assertEquals(0L, (long)msl.getNextSequence());
        assertEquals(1L, (long)msl.getNextSequence());
    }
}
