package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

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

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Before
    @SuppressWarnings("unchecked")
    public void generateStream() throws Exception
    {
        cdr = CorfuDBRuntime.createRuntime("memory");
        instance = cdr.getLocalInstance();
        instance.getConfigurationMaster().resetAll();
        streamID = UUID.randomUUID();
        testTree = instance.openObject(streamID, LambdaLogicalBTree.class);
    }

  //  @Test
    public void treeIsPuttableGettable()
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        assertThat(testTree.get("key0"))
                .isEqualTo("abcd");
        assertThat(testTree.get("key1"))
                .isEqualTo("efgh");
    }


  //  @Test
    public void multipleTreesContainSameData() throws Exception
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        LambdaLogicalBTree<String, String> tree2 = instance.openObject(streamID, LambdaLogicalBTree.class);
        assertThat(tree2.get("key0"))
                .isEqualTo("abcd");
        assertThat(tree2.get("key1"))
                .isEqualTo("efgh");
    }

   // @Test
    public void ensureMutatorAccessorsWork() throws Exception
    {
        testTree.put("key0", "abcd");
        testTree.put("key1", "xxxx");
        assertThat(testTree.get("key1")).isEqualTo("xxxx");
    }

   // @Test
    public void DeferredTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        testTree.put("key0", "abcd");
        final LambdaLogicalBTree<String, String> txMap = testTree;
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

   // @Test
    public void crossMapSwapTransactionalTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        IStream s2 = instance.openStream(UUID.randomUUID());
        LambdaLogicalBTree<String, String> testMap2 =
                instance.openObject(UUID.randomUUID(), LambdaLogicalBTree.class);

        testTree.put("key0", "abcd");
        testMap2.put("key0", "xxxx");
        final LambdaLogicalBTree<String, String> txMap = testTree;
        final LambdaLogicalBTree<String, String> txMap2 = testMap2;

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

  //  @Test
    public void mapOfMapsTest() throws Exception
    {
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        LambdaLogicalBTree<String, LambdaLogicalBTree<String, String>> tmap2 =
                instance.openObject(UUID.randomUUID(), LambdaLogicalBTree.class);
        tmap2.put("abcd", testTree);
        testTree.put("key0", "abcd2");
        assertThat(tmap2.get("abcd").get("key0"))
                .isEqualTo("abcd2");
    }
/*
    @Test
    public void OpaqueDeferredTransactionalTest() throws Exception
    {
        OpaqueDeferredTransaction tx = new OpaqueDeferredTransaction(cdr.getLocalInstance());
        testTree.put("key0", "abcd");
        final LambdaLogicalBTree<String, String> txMap = testTree;
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
        OpaqueDeferredTransaction txAbort = new OpaqueDeferredTransaction(cdr.getLocalInstance());
        final LambdaLogicalBTree<String, String> txMap2 = testTree;
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
*/
  //  @Test
    public void TimeTravelSMRTest()
    {
        LambdaLogicalBTree<Integer, Integer> map = instance.openObject(UUID.randomUUID(),
                new ICorfuDBInstance.OpenObjectArgs<LambdaLogicalBTree>(LambdaLogicalBTree.class, TimeTravelSMREngine.class));

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

   // @Test
    public void NonTimeTravelSMRTest()
    {
        LambdaLogicalBTree<Integer, Integer> map = instance.openObject(UUID.randomUUID(),
                new ICorfuDBInstance.OpenObjectArgs<LambdaLogicalBTree>(LambdaLogicalBTree.class, SimpleSMREngine.class));


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
