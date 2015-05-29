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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by crossbach on 5/27/15.
 */
public class BTreeTest {

    IStream s;
    ICorfuDBInstance instance;
    AbstractLambdaBTree<String, String> testTree;
    UUID streamID;
    CorfuDBRuntime cdr;
    int B = 4;

    public AbstractLambdaBTree createTree(Class cls, IStream s) { return createTree(cls, new Object[] { s }); }
    public AbstractLambdaBTree createTree(Class cls, IStream s, Class smrc) { return createTree(cls, new Object[] { s, smrc }); }
    public AbstractLambdaBTree createTree(Class cls, Object[] args) {
        Constructor ctor = findConstructor(cls, args);
        try {
            AbstractLambdaBTree tree = (AbstractLambdaBTree) ctor.newInstance(args);
            tree.init();
            return tree;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }



    @Before
    public void generateStream() throws Exception
    {
        cdr = new CorfuDBRuntime("memory");
        ConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        instance = cdr.getLocalInstance();
        streamID = UUID.randomUUID();
        s = instance.openStream(streamID);
    }

    @Test public void treeIsPuttableGettableLogical() { treeIsPuttableGettable(LambdaLogicalBTree.class);}
    @Test public void treeIsPuttableGettablePhysical() { treeIsPuttableGettable(LPBTree.class);}

    public void treeIsPuttableGettable(Class treeclass)
    {
        testTree = createTree(treeclass, s);
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        assertThat(testTree.get("key0"))
                .isEqualTo("abcd");
        assertThat(testTree.get("key1"))
                .isEqualTo("efgh");
    }

    @Test public void multipleTreesContainSameDataLogical() throws Exception { multipleTreesContainSameData(LambdaLogicalBTree.class);}
    @Test public void multipleTreesContainSameDataPhysical() throws Exception { multipleTreesContainSameData(LPBTree.class); }

    public void multipleTreesContainSameData(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        testTree.put("key0", "abcd");
        testTree.put("key1", "efgh");
        IStream s2 = cdr.openStream(streamID, SimpleStream.class);
        IBTree<String, String> tree2 = createTree(treeclass, s2);
        assertThat(tree2.get("key0"))
                .isEqualTo("abcd");
        assertThat(tree2.get("key1"))
                .isEqualTo("efgh");
    }

    @Test public void ensureMutatorAccessorsWorkLogical() throws Exception { ensureMutatorAccessorsWork(LambdaLogicalBTree.class);}
    @Test public void ensureMutatorAccessorsWorkPhysical() throws Exception { ensureMutatorAccessorsWork(LPBTree.class);}

    public void ensureMutatorAccessorsWork(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        testTree.put("key0", "abcd");
        testTree.put("key1", "xxxx");
        assertThat(testTree.get("key1")).isEqualTo("xxxx");
    }

    @Test public void deferredTransactionalTestLogical() throws Exception { deferredTransactionalTest(LambdaLogicalBTree.class);}
    @Test public void deferredTransactionalTestPhysical() throws Exception { deferredTransactionalTest(LPBTree.class);}

    public void deferredTransactionalTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        DeferredTransaction tx = new DeferredTransaction(cdr);
        testTree.put("key0", "abcd");
        final AbstractLambdaBTree<String, String> txMap = testTree.getTransactionalContext(tx);
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

    @Test public void crossMapSwapTransactionalTestLogical() throws Exception { crossMapSwapTransactionalTest(LambdaLogicalBTree.class);}
    @Test public void crossMapSwapTransactionalTestPhysical() throws Exception { crossMapSwapTransactionalTest(LPBTree.class);}

    public void crossMapSwapTransactionalTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        DeferredTransaction tx = new DeferredTransaction(cdr);
        IStream s2 = cdr.openStream(UUID.randomUUID(), SimpleStream.class);
        AbstractLambdaBTree<String, String> testMap2 = createTree(treeclass, s2);

        testTree.put("key0", "abcd");
        testMap2.put("key0", "xxxx");
        final AbstractLambdaBTree<String, String> txMap = testTree.getTransactionalContext(tx);
        final AbstractLambdaBTree<String, String> txMap2 = testMap2.getTransactionalContext(tx);

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

    @Test public void mapOfMapsTestLogical() throws Exception { mapOfMapsTest(LambdaLogicalBTree.class);}
    @Test public void mapOfMapsTestPhysical() throws Exception { mapOfMapsTest(LPBTree.class);}

    public void mapOfMapsTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        DeferredTransaction tx = new DeferredTransaction(cdr);
        IStream s2 = cdr.openStream(UUID.randomUUID(), SimpleStream.class);
        AbstractLambdaBTree<String, AbstractLambdaBTree<String, String>> tmap2 = createTree(treeclass, s2);
        tmap2.put("abcd", testTree);
        testTree.put("key0", "abcd");
        assertThat(tmap2.get("abcd").get("key0"))
                .isEqualTo("abcd");
    }

    @Test public void OpaqueDeferredTransactionalTestLogical() throws Exception { OpaqueDeferredTransactionalTest(LambdaLogicalBTree.class);}
    @Test public void OpaqueDeferredTransactionalTestPhysical() throws Exception { OpaqueDeferredTransactionalTest(LPBTree.class);}

    public void OpaqueDeferredTransactionalTest(Class treeclass) throws Exception {
        testTree = createTree(treeclass, s);
        OpaqueDeferredTransaction tx = new OpaqueDeferredTransaction(cdr);
        testTree.put("key0", "abcd");
        final AbstractLambdaBTree<String, String> txMap = testTree.getTransactionalContext(tx);
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
        final AbstractLambdaBTree<String, String> txMap2 = testTree.getTransactionalContext(tx);
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

    @Test public void TimeTravelSMRTestLogical() { TimeTravelSMRTest(LambdaLogicalBTree.class); }
    @Test public void TimeTravelSMRTestPhysical() { TimeTravelSMRTest(LPBTree.class); }

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

    @Test public void NonTimeTravelSMRTestLogical() { NonTimeTravelSMRTest(LambdaLogicalBTree.class);}
    @Test public void NonTimeTravelSMRTestPhysical() { NonTimeTravelSMRTest(LPBTree.class);}

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
