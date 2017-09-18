package org.corfudb.runtime.object.transactions;

import org.corfudb.annotations.CorfuObject;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ConflictParameterClass;
import org.corfudb.util.serializer.ICorfuHashable;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 11/16/16.
 */
public class OptimisticTransactionContextTest extends AbstractTransactionContextTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }


    /** Checks that the fine-grained conflict set is correctly produced
     * by the annotation framework.
     */
    @Test
    public void checkConflictParameters() {
        ConflictParameterClass testObject = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setType(ConflictParameterClass.class)
                .open();

        final String TEST_0 = "0";
        final String TEST_1 = "1";
        final int TEST_2 = 2;
        final int TEST_3 = 3;
        final String TEST_4 = "4";
        final String TEST_5 = "5";

        getRuntime().getObjectsView().TXBegin();
        // RS=TEST_0
        testObject.accessorTest(TEST_0, TEST_1);
        // WS=TEST_3
        testObject.mutatorTest(TEST_2, TEST_3);
        // WS,RS=TEST_4
        testObject.mutatorAccessorTest(TEST_4, TEST_5);

        // Assert that the conflict set contains TEST_0, TEST_4
        assertThat(Transactions.getContext().getConflictSet().getConflicts()
                .values()
                .stream()
                .flatMap(x -> x.stream())
                .collect(Collectors.toList()))
                .contains(TEST_0, TEST_4);

        // in optimistic mode, assert that the conflict set does NOT contain TEST_1 - TEST_3
        assertThat(Transactions.getContext().getConflictSet()
                .getConflicts().values().stream()
                .flatMap(x -> x.stream())
                .collect(Collectors.toList()))
                .doesNotContain(TEST_1, TEST_2, TEST_3);
        try {
            getRuntime().getObjectsView().TXAbort();
        } catch (TransactionAbortedException tae) {
            // Intended to throw abort.
        }
    }


    @Data
    @AllArgsConstructor
    static class CustomConflictObject {
        final String k1;
        final String k2;
    }

    /** When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently, but when they conflict they should abort.
     */
    @Test
    public void customConflictObjectsConflictAborts()
    {
        CustomConflictObject c1 = new CustomConflictObject("a", "a");
        CustomConflictObject c2 = new CustomConflictObject("a", "a");

        Map<CustomConflictObject, String> map = getDefaultRuntime().getObjectsView()
                        .build()
                        .setTypeToken(new TypeToken<SMRMap<CustomConflictObject, String>>() {})
                        .setStreamName("test")
                        .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
    }

    /** When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently so they do not abort.
     */
    @Test
    public void customConflictObjectsNoConflictNoAbort()
    {
        CustomConflictObject c1 = new CustomConflictObject("a", "a");
        CustomConflictObject c2 = new CustomConflictObject("a", "b");

        Map<CustomConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<CustomConflictObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

  
    @Data
    @AllArgsConstructor
    static class CustomSameHashConflictObject {
        final String k1;
        final String k2;
    }

    /** This test generates a custom object which has been registered
     * with the serializer to always conflict, as it always hashes to
     * and empty byte array.
     */
    @Test
    public void customSameHashAlwaysConflicts() {
        // Register a custom hasher which always hashes to an empty byte array
        Serializers.JSON.registerCustomHasher(CustomSameHashConflictObject.class,
                o -> new byte[0]);

        CustomSameHashConflictObject c1 = new CustomSameHashConflictObject("a", "a");
        CustomSameHashConflictObject c2 = new CustomSameHashConflictObject("a", "b");

        Map<CustomSameHashConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<CustomSameHashConflictObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertThrows()
                .isInstanceOf(TransactionAbortedException.class);
    }

    @Data
    @AllArgsConstructor
    static class CustomHashConflictObject {
        final String k1;
        final String k2;
    }


    /** This test generates a custom object which has been registered
     * with the serializer to use the full value as the conflict hash,
     * and should not conflict.
     */
    @Test
    public void customHasDoesNotConflict() {
        // Register a custom hasher which always hashes to the two strings together as a
        // byte array
        Serializers.JSON.registerCustomHasher(CustomSameHashConflictObject.class,
                o -> {
                    ByteBuffer b = ByteBuffer.wrap(new byte[o.k1.length() + o.k2.length()]);
                    b.put(o.k1.getBytes());
                    b.put(o.k2.getBytes());
                    return b.array();
                });

        CustomHashConflictObject c1 = new CustomHashConflictObject("a", "a");
        CustomHashConflictObject c2 = new CustomHashConflictObject("a", "b");

        Map<CustomHashConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<CustomHashConflictObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    @Data
    @AllArgsConstructor
    static class IHashAlwaysConflictObject implements ICorfuHashable {
        final String k1;
        final String k2;

        @Override
        public byte[] generateCorfuHash() {
            return new byte[0];
        }
    }

    /** This test generates a custom object which implements an interface which always
     * conflicts.
     */
    @Test
    public void IHashAlwaysConflicts() {
        IHashAlwaysConflictObject c1 = new IHashAlwaysConflictObject("a", "a");
        IHashAlwaysConflictObject c2 = new IHashAlwaysConflictObject("a", "b");

        Map<IHashAlwaysConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<IHashAlwaysConflictObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertThrows()
                .isInstanceOf(TransactionAbortedException.class);
    }


    @Data
    @AllArgsConstructor
    static class IHashConflictObject implements ICorfuHashable {
        final String k1;
        final String k2;

        @Override
        public byte[] generateCorfuHash() {
            ByteBuffer b = ByteBuffer.wrap(new byte[k1.length() + k2.length()]);
            b.put(k1.getBytes());
            b.put(k2.getBytes());
            return b.array();
        }
    }

    /** This test generates a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @Test
    public void IHashNoConflicts() {
        IHashConflictObject c1 = new IHashConflictObject("a", "a");
        IHashConflictObject c2 = new IHashConflictObject("a", "b");

        Map<IHashConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<IHashConflictObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    @Data
    static class ExtendedIHashObject extends IHashConflictObject {

        public ExtendedIHashObject(String k1, String k2) {
            super(k1, k2);
        }

        /** A simple dummy method. */
        public String getK1K2() {
            return k1 + k2;
        }
    }

    /** This test extends a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @Test
    public void ExtendedIHashNoConflicts() {
        ExtendedIHashObject c1 = new ExtendedIHashObject("a", "a");
        ExtendedIHashObject c2 = new ExtendedIHashObject("a", "b");

        Map<ExtendedIHashObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<ExtendedIHashObject, String>>() {})
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1 , "v1"));
        t(2, () -> map.put(c2 , "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /** In an optimistic transaction, we should be able to
     *  read our own writes in the same thread.
     */
    @Test
    public void readOwnWrites()
    {
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k" , "v"));
        t(1, () -> get("k"))
                            .assertResult()
                            .isEqualTo("v");
        t(1, this::TXEnd);
    }

    /** We should not be able to read writes written optimistically
     * by other threads.
     */
    @Test
    public void otherThreadCannotReadOptimisticWrites()
    {
        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        // T1 inserts k,v1 optimistically. Other threads
        // should not see this optimistic put.
        t(1, () -> put("k", "v1"));
        // T2 now reads k. It should not see T1's write.
        t(2, () -> get("k"))
                            .assertResult()
                            .isNull();
        // T2 inserts k,v2 optimistically. T1 should not
        // be able to see this write.
        t(2, () -> put("k", "v2"));
        // T1 now reads "k". It should not see T2's write.
        t(1, () -> get("k"))
                            .assertResult()
                            .isNotEqualTo("v2");
    }

    /** This test ensures if modifying multiple keys, with one key that does
     * conflict and another that does not, causes an abort.
     */
    @Test
    public void modifyingMultipleKeysCausesAbort() {
        // T1 modifies k1 and k2.
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k1", "v1"));
        t(1, () -> put("k2", "v2"));

        // T2 modifies k1, commits
        t(2, this::OptimisticTXBegin);
        t(2, () -> put("k1", "v3"));
        t(2, this::TXEnd);

        // T1 commits, should abort
        t(1, this::TXEnd)
                .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
    }

    /** Ensure that, upon two consecutive nested transactions, the latest transaction can
     * see optimistic updates from previous ones.
     *
     */
    @Test
    public void OptimisticStreamGetUpdatedCorrectlyWithNestedTransaction(){
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k", "v0"));

        // Start first nested transaction
        t(1, this::OptimisticTXBegin);
        t(1, () -> get("k"))
                            .assertResult()
                            .isEqualTo("v0");
        t(1, () -> put("k", "v1"));
        t(1, this::TXEnd);
        // End first nested transaction

        // Start second nested transaction
        t(1, this::OptimisticTXBegin);
        t(1, () -> get("k"))
                            .assertResult()
                            .isEqualTo("v1");
        t(1, () -> put("k", "v2"));
        t(1, this::TXEnd);
        // End second nested transaction

        t(1, () -> get("k"))
                            .assertResult()
                            .isEqualTo("v2");
        t(1, this::TXEnd);
        assertThat(getMap())
                .containsEntry("k", "v2");

    }

    /** Threads that start a transaction at the same time
     * (with the same timestamp) should cause one thread
     * to abort while the other succeeds.
     */
    @Test
    public void threadShouldAbortAfterConflict()
    {
        // T1 starts non-transactionally.
        t(1, () -> put("k", "v0"));
        t(1, () -> put("k1", "v1"));
        t(1, () -> put("k2", "v2"));
        // Now T1 and T2 both start transactions and read v0.
        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> get("k"))
                    .assertResult()
                    .isEqualTo("v0");
        t(2, () -> get("k"))
                    .assertResult()
                    .isEqualTo("v0");
        // Now T1 modifies k -> v1 and commits.
        t(1, () -> put("k", "v1"));
        t(1, this::TXEnd);
        // And T2 modifies k -> v2 and tries to commit, but
        // should abort.
        t(2, () -> put("k", "v2"));
        t(2, this::TXEnd)
                    .assertThrows()
                    .isInstanceOf(TransactionAbortedException.class);
        // At the end of the transaction, the map should only
        // contain T1's modification.
        assertThat(getMap())
                .containsEntry("k", "v1");
    }

    /** This test makes sure that a single thread can read
     * its own nested transactions after they have committed,
     * and that nested transactions are committed with the
     * parent transaction.
     */
    @Test
    public void nestedTransactionsCanBeReadDuringCommit() {
        // We start without a transaction and put k,v1
        t(1, () -> put("k", "v1"));
        // Now we start a transaction and put k,v2
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k", "v2"))
                    .assertResult() // put should return the previous value
                    .isEqualTo("v1"); // which is v1.
        // Now we start a nested transaction. It should
        // read v2.
        t(1, this::OptimisticTXBegin);
        t(1, () -> get("k"))
                    .assertResult()
                    .isEqualTo("v2");
        // Now we put k,v3
        t(1, () -> put("k", "v3"))
                    .assertResult()
                    .isEqualTo("v2");   // previous value = v2
        // And then we commit.
        t(1, this::TXEnd);
        // And we should be able to read the nested put
        t(1, () -> get("k"))
                .assertResult()
                .isEqualTo("v3");
        // And we commit the parent transaction.
        t(1, this::TXEnd);

        // And now k,v3 should be in the map.
        assertThat(getMap())
                .containsEntry("k", "v3");
    }

    /** This test makes sure that the nested transactions
     * of two threads are not visible to each other.
     */
    @Test
    public void nestedTransactionsAreIsolatedAcrossThreads() {
        // Start a transaction on both threads.
        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        // Put k, v1 on T1 and k, v2 on T2.
        t(1, () -> put("k", "v1"));
        t(2, () -> put("k", "v2"));
        // Now, start a nested transaction on both threads.
        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        // T1 should see v1 and T2 should see v2.
        t(1, () -> get("k"))
                .assertResult()
                .isEqualTo("v1");
        t(2, () -> get("k"))
                .assertResult()
                .isEqualTo("v2");
        // Now we put k,v3 on T1 and k,v4 on T2
        t(1, () -> put("k", "v3"));
        t(2, () -> put("k", "v4"));
        // And each thread should only see its own modifications.
        t(1, () -> get("k"))
                .assertResult()
                .isEqualTo("v3");
        t(2, () -> get("k"))
                .assertResult()
                .isEqualTo("v4");
        // Now we exit the nested transaction. They should both
        // commit, because they are in optimistic mode.
        t(1, this::TXEnd);
        t(2, this::TXEnd);
        // Check that the parent transaction can only
        // see the correct modifications.
        t(1, () -> get("k"))
                .assertResult()
                .isEqualTo("v3");
        t(2, () -> get("k"))
                .assertResult()
                .isEqualTo("v4");
        // Commit the parent transactions. T2 should abort
        // due to concurrent modification with T1.
        t(1, this::TXEnd);
        t(2, this::TXEnd)
               .assertThrows()
               .isInstanceOf(TransactionAbortedException.class);

        // And the map should contain k,v3 - T1's update.
        assertThat(getMap())
                .containsEntry("k", "v3")
                .doesNotContainEntry("k", "v4");
    }

    /**
     * Check that on abortion of a nested transaction
     * the modifications that happened within it are not
     * leaked into the parent transaction.
     */
    //@Test
    /*
    public void nestedTransactionCanBeAborted() {
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k", "v1"));
        t(1, () -> get("k"))
                        .assertResult()
                        .isEqualTo("v1");
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k", "v2"));
        t(1, () -> get("k"))
                        .assertResult()
                        .isEqualTo("v2");
        t(1, this::TXAbort);
        t(1, () -> get("k"))
                        .assertResult()
                        .isEqualTo("v1");
        t(1, this::TXEnd);
        assertThat(getMap())
                .containsEntry("k", "v1");
    }
    */

    /** This test makes sure that a write-only transaction properly
     * commits its updates, even if there are no accesses
     * during the transaction.
     */
    @Test
    public void writeOnlyTransactionCommitsInMemory() {
        // Write twice to the transaction without a read
        OptimisticTXBegin();
        write("k", "v1");
        write("k", "v2");
        TXEnd();

        // Make sure the object correctly reflects the value
        // of the most recent write.
        assertThat(getMap())
                .containsEntry("k", "v2");
    }


    /** This test checks if modifying two keys from
     *  two different streams will cause a collision.
     *
     *  In the old single-level design, this would cause
     *  a collision since 16 bits of the stream id were
     *  being hashed into the 32 bit hashCode(), so that
     *  certain stream-conflictkey combinations would
     *  collide, as demonstrated below.
     *
     *  TODO: Potentially remove this unit test
     *  TODO: once the hash function has stabilized.
     */
    @Test
    public void collide16Bit() throws Exception {
        CorfuRuntime rt = getDefaultRuntime().connect();
        Map<String, String> m1 = rt.getObjectsView()
                .build()
                .setStreamName("test-1")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();

        Map<String, String> m2 = rt.getObjectsView()
                .build()
                .setStreamName("test-2")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();

        t1(() -> rt.getObjectsView().TXBegin());
        t2(() -> rt.getObjectsView().TXBegin());
        t1(() -> m1.put("azusavnj", "1"));
        t2(() -> m2.put("ajkenmbb", "2"));
        t1(() -> rt.getObjectsView().TXEnd());
        t2(() -> rt.getObjectsView().TXEnd());
    }

    /** Check that precise conflicts do not abort when
     * two conflict keys collide.
     */
    @Test
    public void preciseFalseConflictsDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";

        // Make sure these two keys collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        // Start two transactions, where k1 and k2
        // are written to.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> put(k1, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        t1(() -> TXEnd());

        // The second transaction would normally abort,
        // Even though k1 != k2. However, under precise
        // conflicts, there should be no abort.
        t2(() -> TXEnd())
            .assertDoesNotThrow(TransactionAbortedException.class);
    }



    /** Check that precise conflicts do not abort when
     * two conflict keys collide and the causing operation
     * is a mutator only.
     */
    @Test
    public void preciseFalseConflictsMutatorDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";

        // Make sure these two keys collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        // Start two transactions, where k1 and k2
        // are written to.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> getMap().remove(k1));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        t1(() -> TXEnd());

        // The second transaction would normally abort,
        // Even though k1 != k2. However, under precise
        // conflicts, there should be no abort.
        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do abort
     * when an entry (such as a clear), which
     * conflicts with all updates is inserted.
     */
    @Test
    public void preciseTrueConflictAllAborts() {
        String k1 = "A";
        String k2 = "A";

        // Insert something initially into the map
        put(k1, "a");

        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        // TX1 will clear the map
        // TODO: Until poisoning is implemented, this operation
        // TODO: conflicts when we -scan- the log, but not when
        // TODO: the sequencer is deciding aborts, since "clear"
        // TODO: is not added to the conflict set. We add clear
        // TODO: falsely to the conflict set by inserting k1.
        t1(() -> put(k1, "a"));
        t1(() -> getMap().clear());

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        t1(() -> TXEnd());
        t2(() -> TXEnd())
            .assertThrows()
            .isInstanceOf(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do abort
     * when two conflict keys collide.
     */
    @Test
    public void preciseTrueConflictAllAbort() {
        // Here, k1 == k2, so the transaction should
        // abort.
        String k1 = "A";
        String k2 = "A";

        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> put(k1, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        t1(() -> TXEnd());
        t2(() -> TXEnd())
                .assertThrows()
                .isInstanceOf(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do not abort when
     * two conflict keys collide.
     *
     * Here, k1 and k2 collide, as well as k3 and k4.
     */
    @Test
    public void preciseMultiFalseConflictsDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";
        String k3 = "AaAaAa";
        String k4 = "BBBBBB";

        // Make sure these two key sets collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        assertThat(k3.hashCode())
                .isEqualTo(k4.hashCode());

        // Start two transactions, where k1-k4
        // are written to.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> put(k1, "a"));
        t1(() -> put(k3, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));
        t2(() -> put(k4, "a"));

        t1(() -> TXEnd());

        // The second transaction would normally abort,
        // Even though k1 != k2 and k3 != k4. However, under precise
        // conflicts, there should be no abort.
        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }


    /** Check that precise conflicts do not abort when
     * two conflict keys from two different maps collide.
     *
     * Here, k1 and k2 collide, as well as k3 and k4.
     */
    @Test
    public void preciseMultiStreamFalseConflictsDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";
        String k3 = "AaAaAa";
        String k4 = "BBBBBB";

        Map<String, String> map1 = getRuntime()
                .getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("map1")
                .open();

        Map<String, String> map2 = getRuntime()
                .getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("map2")
                .open();

        // Make sure these two key sets collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        assertThat(k3.hashCode())
                .isEqualTo(k4.hashCode());

        // Start two transactions, where k1-k4
        // are written to on two maps.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> map1.put(k1, "a"));
        t1(() -> map2.put(k3, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> map1.put(k2, "a"));
        t2(() -> map2.put(k4, "a"));

        t1(() -> TXEnd());

        // The second transaction would normally abort,
        // Even though k1 != k2 and k3 != k4. However, under precise
        // conflicts, there should be no abort.
        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }


    /** Check that precise conflicts do not abort when
     * two conflict keys from two different maps collide.
     *
     * The two colliding pairs are written in two different
     * transactions, so they result in two entries written
     * to the log.
     *
     * Here, k1 and k2 collide, as well as k3 and k4.
     */
    @Test
    public void preciseMultiStreamMultiWriteDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";
        String k3 = "AaAaAa";
        String k4 = "BBBBBB";

        Map<String, String> map1 = getRuntime()
                .getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("map1")
                .open();

        Map<String, String> map2 = getRuntime()
                .getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("map2")
                .open();

        // Make sure these two key sets collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        assertThat(k3.hashCode())
                .isEqualTo(k4.hashCode());

        // Start three transactions, where k1-k4
        // are written to on two maps.
        // T1 modifies k1 which falsely conflicts
        // with k2.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> map1.put(k1, "a"));

        // T2 modifies k3 which falsely conflicts
        // with k4.
        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> map2.put(k3, "a"));

        t3(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t3(() -> map1.put(k2, "a"));
        t3(() -> map2.put(k4, "a"));

        t1(() -> TXEnd());
        t2(() -> TXEnd());

        // The third transaction would normally abort,
        // Even though k1 != k2 and k3 != k4. However, under precise
        // conflicts, there should be no abort.
        t3(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do not abort
     * due to a hole fill.
     */
    @Test
    public void preciseFalseConflictDueToHoleFill() {
        // Here, k1 == k2, so the transaction would
        // normally abort, but we will hole fill tx1
        // so it fails

        String k1 = "A";
        String k2 = "A";
        UUID mapId = CorfuRuntime.getStreamID("test stream");

        Map<UUID, Set<byte[]>> conflictMap = ImmutableMap.<UUID, Set<byte[]>>builder()
                .put(mapId, Collections.singleton(Serializers.CORFU.hash(k1)))
                .build();

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        // To simulate a failed transaction,
        // we will manually update the counter
        // on the sequencer.
        getRuntime().getSequencerView()
                .nextToken(Collections.singleton(mapId),1,
                        new TxResolutionInfo(mapId, 0L,
                                conflictMap, conflictMap));

        // Reading address 0L before it gets
        // written will insert a hole without
        // contacting the sequencer.

        getRuntime()
                .getAddressSpaceView()
                .read(0L);

        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }


    /** Check that precise conflicts do not abort when
     * two conflict keys collide.
     */
    @Test
    public void preciseFalseConflictsMixedWithHoleFillDoNotAbort() {
        String k1 = "Aa";
        String k2 = "BB";

        // Make sure these two keys collide.
        assertThat(k1.hashCode())
                .isEqualTo(k2.hashCode());

        UUID mapId = CorfuRuntime.getStreamID("test stream");

        Map<UUID, Set<byte[]>> conflictMap = ImmutableMap.<UUID, Set<byte[]>>builder()
                .put(mapId, Collections.singleton(Serializers.CORFU.hash(k2)))
                .build();

        // Start two transactions, where k1 and k2
        // are written to.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> put(k1, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> put(k2, "a"));

        t1(() -> TXEnd());

        // To simulate a failed transaction T3 that should have,
        // been at address 2 and conflicted with T2
        // we will manually update the counter
        // on the sequencer.
        getRuntime().getSequencerView()
                .nextToken(Collections.singleton(mapId),1,
                        new TxResolutionInfo(mapId, 0L,
                                conflictMap, conflictMap));

        // Reading address 1L before it gets
        // written will insert a hole without
        // contacting the sequencer.
        getRuntime()
                .getAddressSpaceView()
                .read(1L);

        // The second transaction would normally abort,
        // Even though k1 != k2. However, under precise
        // conflicts, there should be no abort.
        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do not abort
     * when not conflicting with a non-transactional
     * update.
     */
    @Test
    public void preciseNonTxConflictsDoNotAbort() {
        // Here, k1 != k2, so the transaction should
        // not abort.
        String k1 = "Aa";
        String k2 = "BB";

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> put(k1, "a"));

        t2(() -> put(k2, "a"));

        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /** Check that precise conflicts do not abort
     * when not conflicting with a non-transactional
     * update.
     */
    @Test
    public void preciseConflictAllAborts() {
        String k1 = "Aa";
        String k2 = "BB";

        Map<String, String> map2 = getRuntime()
                .getObjectsView().build()
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .setStreamName("map2")
                .open();

        // Start two transactions, where k1 and k2
        // are written to.
        t1(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t1(() -> map2.put(k1, "a"));

        t2(() -> getRuntime()
                .getObjectsView()
                .TXBuild()
                .setPreciseConflicts(true)
                .begin());

        t2(() -> map2.size());
        t2(() -> getMap().remove("z"));

        t1(() -> TXEnd());
        t2(() -> TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }
}
