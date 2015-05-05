package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.ITransactionCommand;
import org.corfudb.runtime.smr.OpaqueTransaction;
import org.corfudb.runtime.smr.SimpleTransaction;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.*;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;
/**
 * Created by mwei on 5/1/15.
 */
public class CDBSimpleMapTest {

    SimpleStream s;
    IWriteOnceAddressSpace woas;
    IStreamingSequencer ss;
    CDBSimpleMap<Integer, Integer> testMap;
    UUID streamID;
    CorfuDBRuntime cdr;

    @Before
    public void generateStream() throws Exception
    {
        cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        woas = new WriteOnceAddressSpace(cdr);
        ss = new StreamingSequencer(cdr);
        streamID = UUID.randomUUID();
        s = new SimpleStream(streamID, ss, woas);
        testMap = new CDBSimpleMap<Integer, Integer>(s);
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
        IStream s2 = new SimpleStream(streamID, ss, woas);
        CDBSimpleMap<Integer,Integer> testMap2 = new CDBSimpleMap<Integer,Integer>(s2);
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
    public void simpleTransactionalTest() throws Exception
    {
        SimpleTransaction tx = new SimpleTransaction(cdr);
        testMap.put(10, 100);
        final CDBSimpleMap<Integer,Integer> txMap = testMap.getTransactionalContext(tx);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = txMap.get(10);
            if (result == 100) {
                txMap.put(10, 1000);
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(1000);
    }

    @Test
    public void opaqueTransactionalTest() throws Exception
    {
        OpaqueTransaction tx = new OpaqueTransaction(cdr);
        testMap.put(10, 100);
        final CDBSimpleMap<Integer, Integer> txMap = testMap.getTransactionalContext(tx);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = txMap.get(10);
            if (result == 100) {
                txMap.put(10, 1000);
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(1000);
        OpaqueTransaction txAbort = new OpaqueTransaction(cdr);
        final CDBSimpleMap<Integer, Integer> txMap2 = testMap.getTransactionalContext(tx);
        txAbort.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = txMap2.get(10);
            assertThat(result)
                    .isEqualTo(1000);
            txMap2.put(10, 42);
            result = txMap2.get(10);
            assertThat(result)
                    .isEqualTo(42);
            return false;
        });
        ITimestamp abortStamp = txAbort.propose();
        testMap.getSMREngine().sync(abortStamp);
        assertThat(testMap.get(10))
                .isNotEqualTo(42)
                .isEqualTo(1000);
    }
}
