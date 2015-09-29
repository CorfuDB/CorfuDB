package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.NettyStreamingSequencerServer;
import org.corfudb.infrastructure.StreamingSequencerServer;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.*;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.RandomOpenPort;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;

import java.sql.Time;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
/**
 * Created by mwei on 5/1/15.
 */
@Slf4j
public class CDBSimpleMapTest {

    IStream s;
    static CorfuInfrastructureBuilder infrastructure;
    static ICorfuDBInstance instance;
    static public CDBSimpleMap<Integer, Integer> testMap;
    UUID streamID;
    CorfuDBRuntime cdr;

    @Before
    @SuppressWarnings("unchecked")
    public void generateStream() throws Exception
    {
        infrastructure =
                CorfuInfrastructureBuilder.getBuilder()
                        .addSequencer(RandomOpenPort.getOpenPort(), NettyStreamingSequencerServer.class, "nsss", null)
                        .addLoggingUnit(RandomOpenPort.getOpenPort(), 0, NettyLogUnitServer.class, "nlu", null)
                        .start(RandomOpenPort.getOpenPort());

        cdr = CorfuDBRuntime.getRuntime(infrastructure.getConfigString());
        instance = cdr.getLocalInstance();
        streamID = UUID.randomUUID();
        s = instance.openStream(streamID);
        testMap = instance.openObject(streamID, CDBSimpleMap.class);
    }

    @Test
    public void mapIsPuttableGettable()
    {
        testMap.put(0, 10);
        testMap.put(10, 20);
        assertThat(testMap.get(0))
                .isEqualTo(10);
        assertThat(testMap.get(10))
                .isEqualTo(20);
    }


    @Test
    public void multipleMapsContainSameData() throws Exception
    {
        testMap.put(0, 10);
        testMap.put(10, 100);
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(streamID, CDBSimpleMap.class);
        assertThat(testMap2.get(0))
                .isEqualTo(10);
        assertThat(testMap2.get(10))
                .isEqualTo(100);
    }

    @Test
    public void ensureMutatorAccessorsWork() throws Exception
    {
        testMap.put(0, 10);
        assertThat(testMap.put(0, 100))
                .isEqualTo(10);
    }

    @Test
    public void DeferredTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        testMap.put(10, 100);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = testMapLocal.get(10);
            if (result == 100) {
                testMapLocal.put(10, 1000);
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
//        assert(instance.getAddressSpace().readHints(((SimpleTimestamp)txStamp).address).isSetFlatTxn());
        assertThat(testMap.get(10))
                .isEqualTo(1000);
    }

    @Test
    public void crossMapSwapTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);

        testMap.put(10, 100);
        testMap2.put(10, 1000);

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            log.info("old is {}",old1);
            Integer old2 = testMap2.put(10, old1);
            log.info("old2 is {}", old2);
            testMapLocal.put(10, old2);
            return true;
        });

        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        // Make sure that the hint has been committed
        //assert(instance.getAddressSpace().readHints(((SimpleTimestamp) txStamp).address).isSetFlatTxn());
        testMap2.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(1000);
        assertThat(testMap2.get(10))
                .isEqualTo(100);
    }

    @Test
    public void mapOfMapsTest() throws Exception
    {
        CDBSimpleMap<Integer, CDBSimpleMap<Integer, Integer>> testMap2 =
                instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);
        testMap2.put(10, testMap);
        testMap.put(100, 1000);
        assertThat(testMap2.get(10).get(100))
                .isEqualTo(1000);
    }

    //@Test
    public void OpaqueDeferredTransactionalTest() throws Exception
    {
        OpaqueDeferredTransaction tx = new OpaqueDeferredTransaction(cdr.getLocalInstance());

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        testMap.put(10, 100);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = testMapLocal.get(10);
            if (result == 100) {
                testMapLocal.put(10, 1000);
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testMapLocal.getSMREngine().sync(txStamp);
        assertThat(testMapLocal.get(10))
                .isEqualTo(1000);
        OpaqueDeferredTransaction txAbort = new OpaqueDeferredTransaction(cdr.getLocalInstance());

        txAbort.setTransaction((ITransactionOptions opts) -> {
            Integer result = testMapLocal.get(10);
            assertThat(result)
                    .isEqualTo(1000);
            testMapLocal.put(10, 42);
            result = testMapLocal.get(10);
            assertThat(result)
                    .isEqualTo(42);
            opts.abort();
            return null;
        });
        ITimestamp abortStamp = txAbort.propose();
        testMap.getSMREngine().sync(abortStamp);
        assertThat(testMap.get(10))
                .isNotEqualTo(42)
                .isEqualTo(1000);
    }

    //@Test
    public void TimeTravelSMRTest()
    {
        IStream s1 = instance.openStream(UUID.randomUUID());
        CDBSimpleMap<Integer, Integer> map = instance.openObject(streamID, new ICorfuDBInstance.OpenObjectArgs<CDBSimpleMap>(
                    CDBSimpleMap.class,
                    TimeTravelSMREngine.class
        ));

        map.put(10, 100);
        map.put(100, 1000);
        ITimestamp ts1 = map.getSMREngine().getLastProposal();
        map.put(100, 1234);
        ITimestamp ts2 = map.getSMREngine().getLastProposal();
        map.remove(100);
        ITimestamp ts3 = map.getSMREngine().getLastProposal();
        map.put(100, 5678);
        ITimestamp ts4 = map.getSMREngine().getLastProposal();

        assertThat(map.get(100))
                .isEqualTo(5678);
        assertThat(map.get(10))
                .isEqualTo(100);

        TimeTravelSMREngine t = (TimeTravelSMREngine) map.getSMREngine();
        t.travelAndLock(ts1);
        assertThat(map.get(100))
                .isEqualTo(1000);
        assertThat(map.get(10))
                .isEqualTo(100);

        t.travelAndLock(ts4);
        assertThat(map.get(100))
                .isEqualTo(5678);
        assertThat(map.get(10))
                .isEqualTo(100);

        t.travelAndLock(ts3);
        assertThat(map.get(100))
                .isEqualTo(null);
        assertThat(map.get(10))
                .isEqualTo(100);

        t.travelAndLock(ts2);
        assertThat(map.get(100))
                .isEqualTo(1234);
        assertThat(map.get(10))
                .isEqualTo(100);
    }

    //@Test
    public void doConcurrentGets() {
     //   instance.getConfigurationMaster().resetAll();
        JUnitCore.runClasses(ParallelComputer.methods(), ConcurrentGets.class);
    }

    public static class ConcurrentGets {

        UUID streamID;
        Integer keys = 5000;

        public ConcurrentGets() {
            streamID = UUID.randomUUID();
            CDBSimpleMap<Integer, String> dspMapInit = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = 0; i < keys; i++)
            {
                dspMapInit.put(i, i.toString());
            }
        }

        @Test
        public void GetLooper0() {
            CDBSimpleMap<Integer, String> dspMap1 = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = 0; i < keys; i++)
            {
                assertThat(dspMap1.get(i))
                    .isEqualTo(i.toString());
            }
        }

        @Test
        public void GetLooper1() {
            CDBSimpleMap<Integer, String> dspMap2 = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = keys-1; i > -1; i--)
            {
                assertThat(dspMap2.get(i))
                        .isEqualTo(i.toString());
            }
        }

        @Test
        public void GetLooperSameKey1() {
            CDBSimpleMap<Integer, String> dspMap3 = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = 0; i < keys; i++)
            {
                assertThat(dspMap3.get(10))
                        .isEqualTo("10");
            }
        }

        @Test
        public void GetLooperSameKey2() {
            CDBSimpleMap<Integer, String> dspMap4 = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = 0; i < keys; i++)
            {
                assertThat(dspMap4.get(10))
                        .isEqualTo("10");
            }
        }

        @Test
        public void GetLooperDifferentKey() {
            CDBSimpleMap<Integer, String> dspMap5 = instance.openObject(streamID, CDBSimpleMap.class);
            for (Integer i = 0; i < keys; i++)
            {
                assertThat(dspMap5.get(30))
                        .isEqualTo("30");
            }

        }
    }
}
