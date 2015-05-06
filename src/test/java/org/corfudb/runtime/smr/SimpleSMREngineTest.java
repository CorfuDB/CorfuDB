package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.*;
import org.junit.Before;
import org.junit.Test;

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
    IWriteOnceAddressSpace woas;
    IStreamingSequencer ss;
    @Before
    public void generateStream()
    {
        CorfuDBRuntime cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        woas = new WriteOnceAddressSpace(cdr);
        ss = new StreamingSequencer(cdr);
        s = new SimpleStream(UUID.randomUUID(), ss, woas, cdr);
    }

    @Test
    public void simpleIntegerSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
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
    public void mutatorAccessorSMRTest() throws Exception
    {
        SimpleSMREngine<AtomicInteger> smr = new SimpleSMREngine<AtomicInteger>(s, AtomicInteger.class);
        ISMREngineCommand<AtomicInteger> getAndIncrement =
                (ISMREngineCommand<AtomicInteger>) (a,o) -> {o.getReturnResult().complete(a.getAndIncrement());};
        CompletableFuture<Object> previous = new CompletableFuture<Object>();

        ITimestamp ts1 = smr.propose(getAndIncrement, previous);
        smr.sync(ts1);
        assertThat(smr.getObject().get())
                .isEqualTo(1);
        assertThat(previous.get())
                .isEqualTo(0);

        previous = new CompletableFuture<Object>();
        ITimestamp ts2 = smr.propose(getAndIncrement, previous);
        smr.sync(ts2);
        assertThat(smr.getObject().get())
                .isEqualTo(2);
        assertThat(previous.get())
                .isEqualTo(1);
    }
}
