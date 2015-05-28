package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by crossbach on 5/27/15.
 */
public class LambdaBTreeTest {

    IStream s;
    ICorfuDBInstance instance;
    LambdaLogicalBTree<String, String> testTree;
    UUID streamID;
    CorfuDBRuntime cdr;
    int B = 4;

    @Before
    public void generateStream() throws Exception
    {
        cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        instance = cdr.getLocalInstance();
        streamID = UUID.randomUUID();
        s = instance.openStream(streamID);
        testTree = new LambdaLogicalBTree<String, String>(s);
    }

    @Test
    public void treeIsPuttableGettable()
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        assertThat(testTree.get("key0"))
                .isEqualTo("abcd");
        assertThat(testTree.get("key1"))
                .isEqualTo("efgh");
    }


    @Test
    public void multipleTreesContainSameData() throws Exception
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        IStream s2 = cdr.openStream(streamID, SimpleStream.class);
        LambdaLogicalBTree<String, String> tree2 = new LambdaLogicalBTree<String, String>(s2);
        assertThat(tree2.get("key0"))
                .isEqualTo("abcd");
        assertThat(tree2.get("key1"))
                .isEqualTo("efgh");
    }

    @Test
    public void ensureMutatorAccessorsWork() throws Exception
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "xxxx");
        assertThat(testTree.get("key1")).isEqualTo("xxxx");
    }

    @Test
    public void DeferredTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr);
        testTree.put("key0", "abcd");
        final LambdaLogicalBTree<String, String> txMap = testTree.getTransactionalContext(tx);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            String result = txMap.get("key0");
            if (result.compareTo("abcd") == 0) {
                txMap.put("key0", "ABCD");
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testTree.getSMREngine().sync(txStamp);
        assertThat(testTree.get("key0"))
                .isEqualTo("ABCD");
    }

    @Test
    public void crossMapSwapTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr);
        IStream s2 = cdr.openStream(UUID.randomUUID(), SimpleStream.class);
        LambdaLogicalBTree<String, String> testMap2 = new LambdaLogicalBTree<String, String>(s2);

        testTree.put("key0", "abcd");
        testMap2.put("key0", "xxxx");
        final LambdaLogicalBTree<String, String> txMap = testTree.getTransactionalContext(tx);
        final LambdaLogicalBTree<String, String> txMap2 = testMap2.getTransactionalContext(tx);

        tx.setTransaction((ITransactionCommand) (opts) -> {
            String old1 = txMap.get("key0");
            String old2 = txMap2.put("key0", old1);
            txMap.update("key0", old2);
            return true;
        });

        ITimestamp txStamp = tx.propose();
        testTree.getSMREngine().sync(txStamp);
        testMap2.getSMREngine().sync(txStamp);
        assertThat(testTree.get("key0"))
                .isEqualTo("xxxx");
        assertThat(testMap2.get("key0"))
                .isEqualTo("abcd");
    }

    @Test
    public void mapOfMapsTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr);
        IStream s2 = cdr.openStream(UUID.randomUUID(), SimpleStream.class);
        LambdaLogicalBTree<String, LambdaLogicalBTree<String, String>> tmap2 = new LambdaLogicalBTree<String, LambdaLogicalBTree<String, String>>(s2);
        tmap2.put("abcd", testTree);
        testTree.put("key0", "abcd");
        assertThat(tmap2.get("abcd").get("key0"))
                .isEqualTo("abcd");
    }

    @Test
    public void OpaqueDeferredTransactionalTest() throws Exception
    {
        OpaqueDeferredTransaction tx = new OpaqueDeferredTransaction(cdr);
        testTree.put("key0", "abcd");
        final LambdaLogicalBTree<String, String> txMap = testTree.getTransactionalContext(tx);
        tx.setTransaction((ITransactionCommand) (opts) -> {
            String result = txMap.get("key0");
            if (result.compareTo("abcd") == 0) {
                txMap.put("key0", "xxxx");
                return true;
            }
            return false;
        });
        ITimestamp txStamp = tx.propose();
        testTree.getSMREngine().sync(txStamp);
        assertThat(testTree.get("key0"))
                .isEqualTo("xxxx");
        OpaqueDeferredTransaction txAbort = new OpaqueDeferredTransaction(cdr);
        final LambdaLogicalBTree<String, String> txMap2 = testTree.getTransactionalContext(tx);
        txAbort.setTransaction((ITransactionCommand) (opts) -> {
            String result = txMap2.get("key0");
            assertThat(result)
                    .isEqualTo("xxxx");
            txMap2.put("key0", "42");
            result = txMap2.get("key0");
            assertThat(result)
                    .isEqualTo("42");
            return false;
        });
        ITimestamp abortStamp = txAbort.propose();
        testTree.getSMREngine().sync(abortStamp);
        assertThat(testTree.get("key0"))
                .isNotEqualTo("42")
                .isEqualTo("xxxx");
    }

    @Test
    public void TimeTravelSMRTest()
    {
        IStream s1 = instance.openStream(UUID.randomUUID());
        LambdaLogicalBTree<Integer, Integer> map = new LambdaLogicalBTree<Integer, Integer>(
                s1,
                TimeTravelSMREngine.class

        );

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

    @Test
    public void NonTimeTravelSMRTest()
    {
        IStream s1 = instance.openStream(UUID.randomUUID());
        LambdaLogicalBTree<Integer, Integer> map = new LambdaLogicalBTree<Integer, Integer>(
                s1,
                SimpleSMREngine.class
        );

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


    }
}
