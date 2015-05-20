package org.corfudb.runtime.protocols.sequencers;

import org.corfudb.runtime.protocols.logunits.RedisLogUnitProtocol;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by mwei on 5/20/15.
 */
public class RedisSequencerProtocolIT {

    RedisSequencerProtocol rsp;

    @Before
    public void getSequencerProtocol() throws Exception {
        rsp = new RedisSequencerProtocol("localhost", 12900, null);
    }

    @Test
    public void checkIfSequencerIsPingable() throws Exception
    {
        assertTrue(rsp.ping());
    }

    @Test
    public void checkIfTokensMonotonicallyIncrement() throws Exception
    {
        long seq = rsp.sequenceGetNext();
        for (int i = 0; i < 100; i++)
        {
            long next = rsp.sequenceGetNext();
            assertThat(next)
                    .isGreaterThan(seq);
            seq = next;
        }
    }

    @Test
    public void checkIfTokenGroupsDontOverlap() throws Exception
    {
        long seq = rsp.sequenceGetNext();
        for (int i = 1; i < 100; i++)
        {
            long next = rsp.sequenceGetNext(i);
            assertThat(next)
                    .isGreaterThan(seq);
            seq = next;
        }
    }

    @Test
    public void checkIfCurrentTokenIsCurrent() throws Exception
    {
        long seq = rsp.sequenceGetCurrent();
        assertThat(rsp.sequenceGetCurrent())
                .isEqualTo(rsp.sequenceGetNext(0))
                .isEqualTo(seq);
    }
}
