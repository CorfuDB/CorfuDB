package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntimeIT;
import org.corfudb.runtime.stream.NewStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 5/1/15.
 */
public class SimpleSMREngineTest {

    NewStream s;
    ICorfuDBInstance instance;

   // @Before
    public void generateStream()
    {
        instance = CorfuDBRuntimeIT.generateInstance();
        s = (SimpleStream) instance.openStream(UUID.randomUUID());
    }

  //  @Test
    public void simpleIntegerSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> increment = (a,o) -> a.getAndIncrement();
        ISMREngineCommand<AtomicInteger, Integer> decrement =  (a,o) -> a.getAndDecrement();
        /*
        ITimestamp ts1 = smr.propose(increment, null);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);

        ITimestamp ts2 = smr.propose(decrement, null);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(0);*/
    }

  //  @Test
    public void mutatorAccessorSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> getAndIncrement = (a,o) -> a.getAndIncrement();
        CompletableFuture<Integer> previous = new CompletableFuture<Integer>();

        /*
        ITimestamp ts1 = smr.propose(getAndIncrement, previous);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);
        assertThat(previous.get())
                .isEqualTo(0);

        previous = new CompletableFuture<Integer>();
        ITimestamp ts2 = smr.propose(getAndIncrement, previous);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(2);
        assertThat(previous.get())
                .isEqualTo(1);*/
    }
}
