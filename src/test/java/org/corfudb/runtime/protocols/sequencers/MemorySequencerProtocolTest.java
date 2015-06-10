package org.corfudb.runtime.protocols.sequencers;

import org.corfudb.runtime.protocols.logunits.MemoryLogUnitProtocol;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by mwei on 4/30/15.
 */
public class MemorySequencerProtocolTest {
/*
    @Test
    public void checkIfSequencerIsAlwaysPingable() throws Exception
    {
        MemorySequencerProtocol msp = new MemorySequencerProtocol();
        assertTrue(msp.ping());
    }

    @Test
    public void tokensAlwaysIncrement() throws Exception {
        MemorySequencerProtocol msp = new MemorySequencerProtocol();
        assertEquals(msp.sequenceGetNext(), 0);
        assertEquals(msp.sequenceGetNext(), 1);
        for (int i = 1; i < 100; i++)
        {
            assertEquals(msp.sequenceGetNext(), 1 + i);
        }
    }

    @Test
    public void tokenIncrementsByStride() throws Exception {
        MemorySequencerProtocol msp = new MemorySequencerProtocol();
        assertEquals(msp.sequenceGetNext(1), 0);
        assertEquals(msp.sequenceGetNext(2), 1);
        long last = 3;
        for (int i = 1; i < 100; i++) {
            long current = msp.sequenceGetNext(i);
            assertEquals(last+(i-1), current);
            last = current;
        }
    }

    @Test
    public void tokenReturnCurrent() throws Exception {
        MemorySequencerProtocol msp = new MemorySequencerProtocol();
        assertEquals(msp.sequenceGetNext(), 0);
        assertEquals(msp.sequenceGetCurrent(), 1);
        assertEquals(msp.sequenceGetNext(), 1);
        assertEquals(msp.sequenceGetCurrent(), 2);
        for (int i = 1; i < 100; i++)
        {
            assertEquals(msp.sequenceGetNext(), 1 + i);
            assertEquals(msp.sequenceGetCurrent(), 2 + i);
        }
    }
*/
}
