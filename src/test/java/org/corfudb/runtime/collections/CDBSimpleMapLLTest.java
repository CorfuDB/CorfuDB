package org.corfudb.runtime.collections;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testing the LLTransaction
 *
 * Created by taia on 6/14/15.
 */
public class CDBSimpleMapLLTest {

    IStream s;
    ICorfuDBInstance instance;
    static public CDBSimpleMap<Integer, Integer> testMap;
    UUID streamID;
    CorfuDBRuntime cdr;

    public void checkTxDec(long address, boolean expected) throws Exception {
        Hints hint = instance.getAddressSpace().readHints(address);
        assert(hint.isSetTxDec());
        assertThat(hint.isTxDec()).isEqualTo(expected);
    }

   // @Before
    public void generateStream() throws Exception
    {
        cdr = CorfuDBRuntime.createRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        instance = cdr.getLocalInstance();
        streamID = UUID.randomUUID();
        s = instance.openStream(streamID);
        testMap = instance.openObject(streamID, CDBSimpleMap.class);
    }

  //  @Test
    public void LLTransactionalTest() throws Exception
    {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());
        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        testMap.put(10, 100);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer result = testMapLocal.get(10);
            if (result == 100) {
                testMapLocal.put(10, 1000);
                testMapLocal.put(100, 1000);
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(1000);
        assertThat(testMap.size())
                .isEqualTo(2);
    }

  //  @Test
    public void twoMapLLTransactionalTest() throws Exception
    {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);

        testMap.put(10, 100);
        testMap2.put(10, 1000);

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });

        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        testMap2.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(1000);
        assertThat(testMap2.get(10))
                .isEqualTo(100);

        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, true);
    }

   // @Test
    public void backToBackLLTransactionalTest() throws Exception
    {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());
        LLTransaction tx2 = new LLTransaction(cdr.getLocalInstance());
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);

        testMap.put(10, 100);
        testMap2.put(10, 1000);

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });
        ITimestamp txStamp = tx.propose();
        testMap.getSMREngine().sync(txStamp);
        testMap2.getSMREngine().sync(txStamp);

        tx2.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });
        ITimestamp txStamp2 = tx2.propose();

        assertThat(testMap.get(10))
                .isEqualTo(100);
        assertThat(testMap2.get(10))
                .isEqualTo(1000);

        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, true);
        checkTxDec(((SimpleTimestamp) txStamp2).address, true);
    }

  //  @Test
    public void abortLLTransactionalTest() throws Exception
    {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());
        LLTransaction tx2 = new LLTransaction(cdr.getLocalInstance());
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);

        testMap.put(10, 100);
        testMap2.put(10, 1000);

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });
        tx2.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });
        ITimestamp txStamp = tx.propose();
        ITimestamp txStamp2 = tx2.propose();
        testMap.getSMREngine().sync(txStamp2);
        testMap2.getSMREngine().sync(txStamp2);

        assertThat(testMap.get(10))
                .isEqualTo(1000);
        assertThat(testMap2.get(10))
                .isEqualTo(100);

        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, true);
        checkTxDec(((SimpleTimestamp) txStamp2).address, false);
    }

    // Intervening command should abort the transaction.
   // @Test
    public void LLAbortTest() throws Exception
    {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);

        testMap.put(10, 100);
        testMap2.put(10, 1000);

        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            Integer old1 = testMapLocal.get(10);
            Integer old2 = testMap2.put(10, old1);
            testMapLocal.put(10, old2);
            return true;
        });
        // Artifically wedge another entry in between, where we will insert a write that invalidates txn's readset.
        // This breaks all kinds of abstraction..
        ITimestamp ts = new SimpleTimestamp(instance.getSequencer().getNext());

        ITimestamp txStamp = tx.propose();
        s.write(ts, (ISMREngineCommand) ((map, opts) -> {return ((Map)map).put(10, 200);}));

        testMap.getSMREngine().sync(txStamp);
        testMap2.getSMREngine().sync(txStamp);
        assertThat(testMap.get(10))
                .isEqualTo(200);
        assertThat(testMap2.get(10))
                .isEqualTo(1000);

        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, false);
    }

   // @Test
    public void readSetTest() throws Exception {
        // Make sure we accurately determine what should go in the readset and what should not
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());

        testMap.put(10, 100);
        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            testMapLocal.clear();
            return null;
        });

        ITimestamp txStamp = tx.propose();
        assertThat(tx.getReadSet().size()).isEqualTo(0);
        testMap.getSMREngine().sync(txStamp);
        assertThat(testMap.size()).isEqualTo(0);

        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, true);
    }

   // @Test
    public void readSetOneTest() throws Exception {
        LLTransaction tx = new LLTransaction(cdr.getLocalInstance());

        testMap.put(10, 100);
        CDBSimpleMap<Integer,Integer> testMap2 = instance.openObject(UUID.randomUUID(), CDBSimpleMap.class);
        final CDBSimpleMap<Integer, Integer> testMapLocal = testMap;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            testMapLocal.clear();
            testMap2.put(10,1000);
            return null;
        });

        ITimestamp txStamp = tx.propose();
        assertThat(tx.getReadSet().size()).isEqualTo(1);
        testMap.getSMREngine().sync(txStamp);
        testMap2.getSMREngine().sync(txStamp);
        assertThat(testMap2.get(10)).isEqualTo(1000);


        // Make sure TxDec is now set
        checkTxDec(((SimpleTimestamp) txStamp).address, true);
    }
}
