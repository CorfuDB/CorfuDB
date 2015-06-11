package org.corfudb.infrastructure;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SimpleSequencerServerTest {

    SimpleSequencerServer ss;

    @Before
    public void setUp() {
        ss = new SimpleSequencerServer();
    }

    @Test
    public void acquireSingleToken() throws Exception {
        ss.nextpos(1);
    }

    @Test
    public void tokensAlwaysIncrement() throws Exception {
        assertEquals(ss.nextpos(1), 0);
        assertEquals(ss.nextpos(1), 1);
        for (int i = 1; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), 1 + i);
        }
    }

    @Test
    public void tokenIncrementsByStride() throws Exception {
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
        for (int i = 0; i < 100; i++)
        {
            assertEquals(ss.nextpos(1), i);
        }
        ss.reset();
        assertEquals(ss.nextpos(1), 0);
    }

}
