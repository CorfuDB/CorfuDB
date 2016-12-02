package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.runtime.view.StreamsView;
import org.junit.Test;
import org.omg.CORBA.INITIALIZE;
import org.omg.CORBA.TIMEOUT;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/11/16.
 */
public class CompileProxyTest extends AbstractViewTest {

    @Test
    public void testMap() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                                    .getObjectsView().build()
                                    .setStreamName("my stream")
                                    .setUseCompiledClass(true)
                                    .setTypeToken(new TypeToken<SMRMap<String,String>>() {})
                                    .open();

        getDefaultRuntime().getObjectsView().TXBegin();
        map.put("hello", "world");
        map.put("hell", "world");
        getDefaultRuntime().getObjectsView().TXEnd();

        assertThat(map)
                .containsEntry("hello", "world");
        assertThat(map)
                .containsEntry("hell", "world");
    }

    @Test
    public void testIntegerSimple() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        sharedCounter.setValue(33);
        assertThat(sharedCounter.getValue())
                .isEqualTo(33);
    }

    @Test
    public void testCorfuSharedCounterConcurrent() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();
        int concurrencylevel = 100;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduleConcurrently(concurrencylevel, t ->
            sharedCounter.setValue(t)
        );
        executeScheduled(concurrencylevel, 10000, TimeUnit.MILLISECONDS);

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        assertThat(objStream.check())
                .isEqualTo(concurrencylevel);

        int beforeSync, afterSync;

        // before sync'ing the in-memory object, the in-memory copy does not get updated
        assertThat(beforeSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                .isEqualTo(-1);

        // after sync'ing the in-memory map object, the in-memory map has all the keys
        for (int timestamp = 1; timestamp <= concurrencylevel; timestamp++) {
            proxy_CORFUSMR.syncObjectUnsafe(proxy_CORFUSMR.getUnderlyingObject(), timestamp);
            assertThat((afterSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue()))
                    .isBetween(0, concurrencylevel);
            assertThat(beforeSync)
                    .isNotEqualTo(afterSync);
            beforeSync = afterSync;
        }


        // now we get the LATEST value through the Corfu object API
        assertThat((afterSync = sharedCounter.getValue()))
                .isBetween(0, concurrencylevel);
        assertThat(beforeSync)
                .isEqualTo(afterSync);

        int curValue = sharedCounter.getValue();
        AtomicInteger casSucceeded = new AtomicInteger(0);
        scheduleConcurrently(100, t -> {
                    if (sharedCounter.CAS(curValue, t) == curValue)
                        casSucceeded.incrementAndGet();
        });
        executeScheduled(100, 1000, TimeUnit.MILLISECONDS);
        assertThat(sharedCounter.getValue())
                .isBetween(0, 100);
        assertThat(casSucceeded.get())
                .isEqualTo(1);
    }

    @Test
    public void testCorfuMapConcurrency() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();

        scheduleConcurrently(100, t -> {
            map.put(t.toString(), "world");
        });
        executeScheduled(100, 1000, TimeUnit.MILLISECONDS);


        for (int i = 0; i < 100; i++)
            assertThat(map.containsKey(Integer.toString(i)))
                    .isTrue();

        assertThat(map.containsKey(Integer.toString(100)))
                .isFalse();

    }

}
