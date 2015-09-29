package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.configmasters.MemoryConfigMasterProtocol;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.*;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.*;
/**
 * Created by mwei on 5/1/15.
 */
public class SimpleSMREngineTest {

    SimpleStream s;
    ICorfuDBInstance instance;

   // @Before
    public void generateStream()
    {
        MemoryConfigMasterProtocol.inMemoryClear();
        CorfuDBRuntime cdr = CorfuDBRuntime.createRuntime("memory");
        instance = cdr.getLocalInstance();
        s = (SimpleStream) instance.openStream(UUID.randomUUID());
    }

  //  @Test
    public void simpleIntegerSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> increment = (a,o) -> a.getAndIncrement();
        ISMREngineCommand<AtomicInteger, Integer> decrement =  (a,o) -> a.getAndDecrement();
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
    public void mutatorAccessorSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger, Integer> getAndIncrement = (a,o) -> a.getAndIncrement();
        CompletableFuture<Integer> previous = new CompletableFuture<Integer>();

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
                .isEqualTo(1);
    }
}
