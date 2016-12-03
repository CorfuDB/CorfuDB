package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
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
    public void testSharedCounterSimple() throws Exception {
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
    public void testCorfuSharedCounterConcurrentWrites() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();
        int concurrency = 10;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduleConcurrently(concurrency, t ->
            sharedCounter.setValue(t)
        );
        executeScheduled(concurrency, 10000, TimeUnit.MILLISECONDS);

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        assertThat(objStream.check())
                .isEqualTo(concurrency);

        int beforeSync, afterSync;

        // before sync'ing the in-memory object, the in-memory copy does not get updated
        assertThat(beforeSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                .isEqualTo(-1);

        // after sync'ing the in-memory map object, the in-memory map has all the keys
        for (int timestamp = 1; timestamp <= concurrency; timestamp++) {
            proxy_CORFUSMR.syncObjectUnsafe(proxy_CORFUSMR.getUnderlyingObject(), timestamp);
            assertThat((afterSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue()))
                    .isBetween(0, concurrency);
            assertThat(beforeSync)
                    .isNotEqualTo(afterSync);
            beforeSync = afterSync;
        }


        // now we get the LATEST value through the Corfu object API
        assertThat((afterSync = sharedCounter.getValue()))
                .isBetween(0, concurrency);
        assertThat(beforeSync)
                .isEqualTo(afterSync);

        int curValue = sharedCounter.getValue();
        AtomicInteger casSucceeded = new AtomicInteger(0);
        scheduleConcurrently(concurrency, t -> {
                    if (sharedCounter.CAS(curValue, t) == curValue)
                        casSucceeded.incrementAndGet();
        });
        executeScheduled(concurrency, 1000, TimeUnit.MILLISECONDS);
        assertThat(sharedCounter.getValue())
                .isBetween(0, concurrency);
        assertThat(casSucceeded.get())
                .isEqualTo(1);
    }

    @Test
    public void testCorfuSharedCounterConcurrentReads() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();
        int concurrency = 10;
        int writerwork = 10000;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduleConcurrently(5, t -> {
                    for (int i = 0; i < writerwork; i++)
                        sharedCounter.setValue(t*writerwork + i);
                }
        );
        scheduleConcurrently(concurrency-1, t -> {
                    int lastread = -1;
                    for (int i = 0; i < 1000; i++) {
                        int res = sharedCounter.getValue();
                        boolean assertflag =
                                (
                                        ( ((lastread < writerwork && res < writerwork) || (lastread >= writerwork && res >= writerwork) ) && lastread <= res ) ||
                                            ( (lastread < writerwork && res >= writerwork) || (lastread >= writerwork && res < writerwork) )
                                );
                        assertThat(assertflag)
                                .isTrue();
                    }
                }
        );
        executeScheduled(concurrency, 50000, TimeUnit.MILLISECONDS);

    }


    @Test
    public void testCorfuSharedCounterConcurrentMixedReadsWrites() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        for (int repetition = 0; repetition < 1000; repetition += 2) {
            final int r = repetition;
            t(3, () -> sharedCounter.setValue(r+1));
            t(4, () -> {
                assertThat(objStream.check())
                        .isEqualTo(r);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                        .isEqualTo(r);
            });
            t(3, () -> sharedCounter.setValue(r+2));
            t(4, () -> {
                assertThat(objStream.check())
                        .isEqualTo(r+1);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                        .isEqualTo(r);

                assertThat(sharedCounter.getValue())
                        .isEqualTo(r+2);
            });
        }
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
        int concurrency = 10;

        scheduleConcurrently(concurrency, t -> {
            map.put(t.toString(), "world");
        });
        executeScheduled(concurrency, 1000, TimeUnit.MILLISECONDS);


        for (int i = 0; i < concurrency; i++)
            assertThat(map.containsKey(Integer.toString(i)))
                    .isTrue();

        assertThat(map.containsKey(Integer.toString(concurrency)))
                .isFalse();

    }

    @Test
    public void testCorfuCompoundObjectSimple() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();
        inner.setFirstName("A");
        inner.setLastName("B");
        sharedCorfuCompound.set(inner, 33);

        assertThat(sharedCorfuCompound.getID())
                .isEqualTo(33);
        assertThat(sharedCorfuCompound.getUser().firstName)
                .isEqualTo("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .isEqualTo("B");
    }

    @Test
    public void testCorfuCompoundObjectConcurrentWrites() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        int concurrency = 10;
        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();

        scheduleConcurrently(concurrency, t -> {
            inner.setFirstName("A" + t);
            inner.setLastName("B" + t);
            sharedCorfuCompound.set(inner, t);
        });
        executeScheduled(concurrency, 1000, TimeUnit.MILLISECONDS);

        assertThat(sharedCorfuCompound.getID())
                .isBetween(0, concurrency);

        assertThat(sharedCorfuCompound.getUser().firstName)
                .startsWith("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .startsWith("B");
    }


    @Test
    public void testCorfuCompoundObjectConcurrentMixed() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        ICorfuSMR<CorfuCompoundObj> compiledCorfuCompound = (ICorfuSMR<CorfuCompoundObj>) sharedCorfuCompound;
        ICorfuSMRProxyInternal<CorfuCompoundObj> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuCompoundObj>) compiledCorfuCompound.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();

        inner.setFirstName("E" + 0);
        inner.setLastName("F" + 0);
        sharedCorfuCompound.set(inner, 0);
        sharedCorfuCompound.getID();

        for (int repetition = 0; repetition < 1000; repetition += 2) {
            final int r = repetition;
            CorfuCompoundObj.Inner inn = sharedCorfuCompound.new Inner();
            inn.setFirstName("C" + (r+1));
            inn.setLastName("D" + (r+1));
            t(1, () -> sharedCorfuCompound.set(inn, r+1));
            t(2, () -> {
                assertThat(objStream.check())
                    .isEqualTo(r+1);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getFirstName())
                        .startsWith("E" + r);
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getLastName())
                        .startsWith("F" + r);
            });

            inn.setFirstName("E" + (r+2));
            inn.setLastName("F" + (r+2));
            t(1, () -> sharedCorfuCompound.set(inn, r+2));
            t(2, () -> {
                assertThat(objStream.check())
                        .isEqualTo(r+2);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getFirstName())
                        .startsWith("E" + r);
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getLastName())
                        .startsWith("F" + r);

                assertThat(sharedCorfuCompound.getUser().getFirstName())
                        .startsWith("E" + (r+2));
                assertThat(sharedCorfuCompound.getUser().getLastName())
                        .startsWith("F" + (r+2));
            });
        }

    }

}
