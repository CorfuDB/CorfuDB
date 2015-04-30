package org.corfudb.infrastructure;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SimpleSequencerServerTest {

    @Test
    public void tokensAlwaysIncrement() throws Exception {
        SimpleSequencerServer ss = new SimpleSequencerServer();
        assertEquals(ss.nextpos(1), 0);
        assertEquals(ss.nextpos(1), 1);
        for (int i = 1; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), 1 + i);
        }
    }

    @Test
    public void tokenIncrementsByStride() throws Exception {
        SimpleSequencerServer ss = new SimpleSequencerServer();
        assertEquals(ss.nextpos(1), 0);
        assertEquals(ss.nextpos(2), 1);
        long last = 3;
        for (int i = 1; i < 100; i++) {
            long current = ss.nextpos(i);
            assertEquals(last+(i-1), current);
            last = current;
        }
    }

    @Test
    public void tokenReturnCurrent() throws Exception {
        SimpleSequencerServer ss = new SimpleSequencerServer();
        assertEquals(ss.nextpos(1), 0);
        assertEquals(ss.nextpos(0), 1);
        assertEquals(ss.nextpos(1), 1);
        assertEquals(ss.nextpos(0), 2);
        for (int i = 1; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), 1 + i);
            assertEquals(ss.nextpos(0), 2 + i);
        }
    }

    @Test
    public void successfullyRecover() throws Exception {
        SimpleSequencerServer ss = new SimpleSequencerServer();
        for (int i = 0; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), i);
        }
        ss.recover(30);
        assertEquals(ss.nextpos(1), 30);
        assertEquals(ss.nextpos(1), 31);
    }

    @Test
    public void returnsToZeroOnReset() throws Exception {
        SimpleSequencerServer ss = new SimpleSequencerServer();
        for (int i = 0; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), i);
        }
        ss.reset();
        assertEquals(ss.nextpos(1), 0);
    }

}
