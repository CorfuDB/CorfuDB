package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 6/9/15.
 */
public abstract class IBTreeTest {

    IBTree<String, String> testTree;
    UUID thisTree;
    ICorfuDBInstance instance;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    protected abstract IBTree<String, String> getBtree(ICorfuDBInstance instance, UUID stream);

    @Before
    public void setupBtree()
    {
        instance = CorfuDBRuntime.createRuntime("memory").getLocalInstance();
        thisTree = UUID.randomUUID();
        testTree = getBtree(instance, thisTree);
    }

 //   @Test
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
    public void multipleTreesContainSameData() throws Exception {
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        IBTree<String, String> tree2 = getBtree(instance, thisTree);
        assertThat(tree2.get("key0"))
                .isEqualTo("abcd");
        assertThat(tree2.get("key1"))
                .isEqualTo("efgh");
    }

  //  @Test
    public void ensureMutatorAccessorsWork() throws Exception {
        testTree.put("key0", "abcd");
        testTree.put("key1", "xxxx");
        assertThat(testTree.get("key1")).isEqualTo("xxxx");
    }

  //  @Test
    public void deferredTransactionalTest() throws Exception {
        DeferredTransaction tx = new DeferredTransaction(instance);
        testTree.put("key0", "abcd");
        final IBTree<String, String> txMap = testTree;
        tx.setTransaction((ITransactionCommand) (opts) -> {
            String result = txMap.get("key0");
            if (result.compareTo("abcd") == 0) {
                txMap.put("key0", "ABCD");
                return true;
            }
            return false;
        });
       tx.propose();
      //  testTree.getSMREngine().sync(txStamp);
        assertThat(testTree.get("key0"))
                .isEqualTo("ABCD");
    }

    //@Test public void crossMapSwapTransactionalTestLogical() throws Exception { crossMapSwapTransactionalTest(LambdaLogicalBTree.class);}
    //@Test public void crossMapSwapTransactionalTestPhysical() throws Exception { crossMapSwapTransactionalTest(LPBTree.class);}

   // @Test
    public void crossMapSwapTransactionalTest() throws Exception {
        DeferredTransaction tx = new DeferredTransaction(instance);
        IBTree<String,String> testMap2 = getBtree(instance, UUID.randomUUID());

        testTree.put("key0", "abcd");
        testMap2.put("key0", "xxxx");
        final IBTree<String, String> txMap = testTree;
        final IBTree<String, String> txMap2 = testMap2;

        tx.setTransaction((ITransactionCommand) (opts) -> {
            String old1 = txMap.get("key0");
            String old2 = txMap2.put("key0", old1);
            txMap.update("key0", old2);
            return true;
        });

        ITimestamp txStamp = tx.propose();
        assertThat(testTree.get("key0"))
                .isEqualTo("xxxx");
        assertThat(testMap2.get("key0"))
                .isEqualTo("abcd");
    }

    // @Test public void mapOfMapsTestLogical() throws Exception { mapOfMapsTest(LambdaLogicalBTree.class);}
    // @Test public void mapOfMapsTestPhysical() throws Exception { mapOfMapsTest(LPBTree.class);}
/*
    public void mapOfMapsTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        DeferredTransaction tx = new DeferredTransaction(cdr.getLocalInstance());
        IStream s2 = instance.openStream(streamID);
        AbstractLambdaBTree<String, AbstractLambdaBTree<String, String>> tmap2 = createTree(treeclass, s2);
        tmap2.put("abcd", testTree);
        testTree.put("key0", "abcd");
        assertThat(tmap2.get("abcd").get("key0"))
                .isEqualTo("abcd");
    }

    // @Test public void OpaqueDeferredTransactionalTestLogical() throws Exception { OpaqueDeferredTransactionalTest(LambdaLogicalBTree.class);}
    // @Test public void OpaqueDeferredTransactionalTestPhysical() throws Exception { OpaqueDeferredTransactionalTest(LPBTree.class);}
/*
    public void OpaqueDeferredTransactionalTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        OpaqueDeferredTransaction tx = new OpaqueDeferredTransaction(cdr.getLocalInstance());
        testTree.put("key0", "abcd");
        final AbstractLambdaBTree<String, String> txMap = testTree;
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
        final AbstractLambdaBTree<String, String> txMap2 = testTree;
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

    // @Test public void TimeTravelSMRTestLogical() { TimeTravelSMRTest(LambdaLogicalBTree.class); }
    // @Test public void TimeTravelSMRTestPhysical() { TimeTravelSMRTest(LPBTree.class); }
/*
    public void TimeTravelSMRTest(Class treeclass) {
        testTree = createTree(treeclass, s);
        IStream s1 = instance.openStream(UUID.randomUUID());
        AbstractLambdaBTree<Integer, Integer> map = createTree(
                treeclass,
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

    // @Test public void NonTimeTravelSMRTestLogical() { NonTimeTravelSMRTest(LambdaLogicalBTree.class);}
    // @Test public void NonTimeTravelSMRTestPhysical() { NonTimeTravelSMRTest(LPBTree.class);}
/*
    public void NonTimeTravelSMRTest(Class treeclass) {
        testTree = createTree(treeclass, s);
        IStream s1 = instance.openStream(UUID.randomUUID());
        AbstractLambdaBTree<Integer, Integer> map = createTree(
                treeclass,
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
*/


    /**
     * find the appropriate constructor for the underlying object,
     * given the ctor args supplied in initArgs. This is something every
     * SMREngine implementation needs to be able to do (first pass implementation
     * appeared to assume that underlying objects always have a default ctor,
     * which is untenable.)
     * @param c -- class of underlying object
     * @param initArgs -- ctor args for the desired constructor
     * @param <T> --
     * @return the constructor matching the given argument list if one exists
     */
    <T> Constructor<T> findConstructor(Class<T> c, Object[] initArgs) {
        if (initArgs == null)
            initArgs = new Object[0];
        for (Constructor con : c.getDeclaredConstructors()) {
            Class[] types = con.getParameterTypes();
            if (types.length != initArgs.length)
                continue;
            boolean match = true;
            for (int i = 0; i < types.length; i++) {
                Class need = types[i], got = initArgs[i].getClass();
                if (!need.isAssignableFrom(got)) {
                    if (need.isPrimitive()) {
                        match = (int.class.equals(need) && Integer.class.equals(got))
                                || (long.class.equals(need) && Long.class.equals(got))
                                || (char.class.equals(need) && Character.class.equals(got))
                                || (short.class.equals(need) && Short.class.equals(got))
                                || (boolean.class.equals(need) && Boolean.class.equals(got))
                                || (byte.class.equals(need) && Byte.class.equals(got));
                    } else {
                        match = false;
                    }
                }
                if (!match)
                    break;
            }
            if (match)
                return con;
        }
        throw new IllegalArgumentException("Cannot find an appropriate constructor for class " + c + " and arguments " + Arrays.toString(initArgs));
    }
}
