package org.corfudb.runtime.smr;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.ICorfuDBInstance;
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
    ICorfuDBInstance instance;
    IStream s;

    @Before
    public void createStream()
    {
        MemoryConfigMasterProtocol.inMemoryClear();
        cdr = CorfuDBRuntime.createRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        instance = cdr.getLocalInstance();
        s = instance.openStream(UUID.randomUUID());
    }

    //@Test
    public void simpleIntegerSMRTest() throws Exception
    {
        TimeTravelSMREngine<AtomicInteger> smr = new TimeTravelSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> increment = (a,o) -> a.getAndIncrement();
        ISMREngineCommand<AtomicInteger, Integer> decrement = (a,o) -> a.getAndDecrement();
        ITimestamp ts1 = smr.propose(increment, null);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        ITimestamp ts2 = smr.propose(decrement, null);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(0);
    }

  //  @Test
    public void timeTravelSMRTest() throws Exception
    {
        TimeTravelSMREngine<AtomicInteger> smr = new TimeTravelSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> increment = new ReversibleSMREngineCommand<AtomicInteger, Integer>(
                                        (a,o) -> a.getAndIncrement(),
                                        (a,o) -> a.getAndDecrement()
        );
        ISMREngineCommand<AtomicInteger, Integer> decrement = new ReversibleSMREngineCommand<AtomicInteger, Integer>(
                (a,o) -> a.getAndDecrement(),
                (a,o) -> a.getAndIncrement()
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
