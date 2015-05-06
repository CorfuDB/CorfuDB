package org.corfudb.runtime.smr;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 5/6/15.
 */
public class TimeTravelSMREngineTest {

    CorfuDBRuntime cdr;
    IStream s;

    @Before
    public void createStream()
    {
        cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        s = cdr.openStream(UUID.randomUUID(), SimpleStream.class);
    }

    @Test
    public void simpleIntegerSMRTest() throws Exception
    {
        TimeTravelSMREngine<AtomicInteger> smr = new TimeTravelSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger> increment = (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndIncrement();
        ISMREngineCommand<AtomicInteger> decrement = (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndDecrement();
        ITimestamp ts1 = smr.propose(increment, null);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        ITimestamp ts2 = smr.propose(decrement, null);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(0);
    }

    @Test
    public void timeTravelSMRTest() throws Exception
    {
        TimeTravelSMREngine<AtomicInteger> smr = new TimeTravelSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger> increment = new ReversibleSMREngineCommand<AtomicInteger>(
                                        (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndIncrement(),
                                        (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndDecrement()
        );
        ISMREngineCommand<AtomicInteger> decrement = new ReversibleSMREngineCommand<AtomicInteger>(
                (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndDecrement(),
                (ISMREngineCommand<AtomicInteger>) (a,o) -> a.getAndIncrement()
        );
        ITimestamp ts1 = smr.propose(increment, null);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        ITimestamp ts2 = smr.propose(decrement, null);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(0);

        ITimestamp ts3 = smr.propose(increment, null);
        smr.sync(ts3);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        ITimestamp ts4 = smr.propose(increment, null);
        smr.sync(ts4);
        assertThat(smr.getObject().get())
                .isEqualTo(2);

        smr.travelAndLock(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(0);

        smr.travelAndLock(ts3);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        smr.travelAndLock(ts4);
        assertThat(smr.getObject().get())
                .isEqualTo(2);

        smr.travelAndLock(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);
    }

}
