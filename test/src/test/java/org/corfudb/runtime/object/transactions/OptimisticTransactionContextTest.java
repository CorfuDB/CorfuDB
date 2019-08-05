package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ConflictParameterClass;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.serializer.ICorfuHashable;
import org.corfudb.util.serializer.Serializers;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Created by mwei on 11/16/16.
 */
public class OptimisticTransactionContextTest extends AbstractTransactionContextTest {
    @Override
    public void TXBegin() {
        OptimisticTXBegin();
    }


    /**
     * Checks that the fine-grained conflict set is correctly produced
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
        assertThat(TransactionalContext.getCurrentContext()
                .getReadSetInfo().getConflicts()
                .values()
                .stream()
                .flatMap(x -> x.stream())
                .collect(Collectors.toList()))
                .contains(TEST_0, TEST_4);

        // in optimistic mode, assert that the conflict set does NOT contain TEST_2, TEST_3
        assertThat(TransactionalContext.getCurrentContext()
                .getReadSetInfo()
                .getConflicts().values().stream()
                .flatMap(x -> x.stream())
                .collect(Collectors.toList()))
                .doesNotContain(TEST_2, TEST_3, TEST_5);

        getRuntime().getObjectsView().TXAbort();
    }


    @Data
    @AllArgsConstructor
    static class CustomConflictObject {
        final String k1;
        final String k2;
    }

    /**
     * When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently, but when they conflict they should abort.
     */
    @Test
    public void customConflictObjectsConflictAborts() {
        CustomConflictObject c1 = new CustomConflictObject("a", "a");
        CustomConflictObject c2 = new CustomConflictObject("a", "a");

        Map<CustomConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<CustomConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertThrows()
                .isInstanceOf(TransactionAbortedException.class);
    }

    /**
     * This test checks if optimistic transactions are aborted in the scenario of slow writers and
     * fast readers accessing the same stream, i.e., a reader accesses the address already given to
     * the slow writer before this one gets to actually write into it. This causes the address to
     * be hole filled and therefore the transaction is aborted after several tries.
     * The cause of abort of this transaction is reported as OVERWRITE given that the position
     * was continuously overwritten at the log unit level.
     */
    @Test
    public void checkSlowWriterTxAbortsOnHoleFill() {

        // The default hole fill timeout duration is 10 seconds. In case of aborting a transaction with cause
        // overwrite, 5 write attempts are made. This totals to 50 seconds.
        // Modifying this timeout to 1 second to avoid test timeout after 1 minute.
        final Duration holeFillTimeout = Duration.ofSeconds(1);
        UUID streamID = UUID.randomUUID();
        CorfuRuntime rtWriter = getDefaultRuntime();
        CorfuRuntime rtReader = getNewRuntime(CorfuRuntimeParameters.builder()
                .holeFillTimeout(holeFillTimeout)
                .build())
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();

        Map<String, String> map = rtWriter
                .getObjectsView().build()
                .setStreamID(streamID)
                .setType(SMRMap.class)
                .open();
        // Add rule to force a read on the assigned token before actually writing to that position
        TestRule testRule = new TestRule()
                .matches(m -> {
                    if (m.getMsgType().equals(CorfuMsgType.WRITE)) {
                        rtReader.getStreamsView().get(streamID).next();
                        return true;
                    } else {
                        return false;
                    }
                });
        addClientRule(rtWriter, testRule);

        try {
            OptimisticTXBegin();
            map.put("k1", "v1");
            TXEnd();
            Assert.fail();
        } catch (TransactionAbortedException tae) {
            assertThat(tae.getAbortCause()).isEqualTo(AbortCause.OVERWRITE);
        }

        assertThat(map).doesNotContainKey("k1");
    }

    /**
     * This test checks if optimistic transactions abort in the case where different data is already present
     * in the address given. We force this to happen on all stream layer retries and the transaction
     * should be aborted.
     */
    @Test
    public void checkSlowWriterTxAbortsOnOverwriteDiffData() {
        UUID streamID = UUID.randomUUID();
        CorfuRuntime rtSlowWriter = getDefaultRuntime();
        CorfuRuntime rtIntersect = new CorfuRuntime(getDefaultConfigurationString()).connect();

        Map<String, String> map = rtSlowWriter
                .getObjectsView().build()
                .setStreamID(streamID)
                .setType(SMRMap.class)
                .open();

        int[] retry = new int[1];
        retry[0] = 0;
        // Add rule to force a read on the assigned token before actually writing to that position
        TestRule testRule = new TestRule()
                .matches(m -> {
                    if (m.getMsgType().equals(CorfuMsgType.WRITE) && retry[0] < rtSlowWriter.getParameters().getWriteRetry()) {
                        rtIntersect.getAddressSpaceView().write(new Token(0, retry[0]), "hello world".getBytes());
                        retry[0]++;
                        return true;
                    } else {
                        return false;
                    }
                });
        addClientRule(rtSlowWriter, testRule);

        try {
            OptimisticTXBegin();
            map.put("k2", "v2");
            TXEnd();
            Assert.fail();
        } catch (TransactionAbortedException tae) {
            assertThat(tae.getAbortCause()).isEqualTo(AbortCause.OVERWRITE);
        }

        assertThat(map).doesNotContainKey("k1");
    }

    /**
     * This test checks if optimistic transactions succeed whenever the same data is already present
     * in the given address. This might happen for the case of chain replication and partial
     * writes of a slow writer. A fast reader might propagate its write through the chain, and when
     * attempting to complete the writes find the data already there.
     */
    @Test
    public void checkSlowWriterTxSucceedsOnSameDataOverwrite() {
        UUID streamID = UUID.randomUUID();
        CorfuRuntime rtSlowWriter = getDefaultRuntime();
        CorfuRuntime rtPropagateWrite = new CorfuRuntime(getDefaultConfigurationString()).connect();

        int[] retry = new int[1];
        retry[0] = 0;
        // Add rule to force a read on the assigned token before actually writing to that position
        TestRule testRule = new TestRule()
                .matches(m -> {
                    if (m.getMsgType().equals(CorfuMsgType.WRITE)) {
                        rtPropagateWrite.getAddressSpaceView().write(new Token(0, 0), "hello world".getBytes());
                        retry[0]++;
                        return true;
                    } else {
                        return false;
                    }
                });
        addClientRule(rtSlowWriter, testRule);

        try {
            OptimisticTXBegin();
            rtSlowWriter.getStreamsView().append("hello world".getBytes(), null, streamID);
            TXEnd();
        } catch (TransactionAbortedException tae) {
            Assert.fail();
        }
    }

    /**
     * When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently so they do not abort.
     */
    @Test
    public void customConflictObjectsNoConflictNoAbort() {
        CustomConflictObject c1 = new CustomConflictObject("a", "a");
        CustomConflictObject c2 = new CustomConflictObject("a", "b");

        Map<CustomConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<CustomConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
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

    /**
     * This test generates a custom object which has been registered
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
                .setTypeToken(new TypeToken<SMRMap<CustomSameHashConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
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


    /**
     * This test generates a custom object which has been registered
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
                .setTypeToken(new TypeToken<SMRMap<CustomHashConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
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

    /**
     * This test generates a custom object which implements an interface which always
     * conflicts.
     */
    @Test
    public void IHashAlwaysConflicts() {
        IHashAlwaysConflictObject c1 = new IHashAlwaysConflictObject("a", "a");
        IHashAlwaysConflictObject c2 = new IHashAlwaysConflictObject("a", "b");

        Map<IHashAlwaysConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<IHashAlwaysConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
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

    /**
     * This test generates a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @Test
    public void IHashNoConflicts() {
        IHashConflictObject c1 = new IHashConflictObject("a", "a");
        IHashConflictObject c2 = new IHashConflictObject("a", "b");

        Map<IHashConflictObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<IHashConflictObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    @Data
    static class ExtendedIHashObject extends IHashConflictObject {

        public ExtendedIHashObject(String k1, String k2) {
            super(k1, k2);
        }

        /**
         * A simple dummy method.
         */
        public String getK1K2() {
            return k1 + k2;
        }
    }

    /**
     * This test extends a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @Test
    public void ExtendedIHashNoConflicts() {
        ExtendedIHashObject c1 = new ExtendedIHashObject("a", "a");
        ExtendedIHashObject c2 = new ExtendedIHashObject("a", "b");

        Map<ExtendedIHashObject, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<ExtendedIHashObject, String>>() {
                })
                .setStreamName("test")
                .open();

        t(1, this::OptimisticTXBegin);
        t(2, this::OptimisticTXBegin);
        t(1, () -> map.put(c1, "v1"));
        t(2, () -> map.put(c2, "v2"));
        t(1, this::TXEnd);
        t(2, this::TXEnd)
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /**
     * In an optimistic transaction, we should be able to
     * read our own writes in the same thread.
     */
    @Test
    public void readOwnWrites() {
        t(1, this::OptimisticTXBegin);
        t(1, () -> put("k", "v"));
        t(1, () -> get("k"))
                .assertResult()
                .isEqualTo("v");
        t(1, this::TXEnd);
    }

    /**
     * We should not be able to read writes written optimistically
     * by other threads.
     */
    @Test
    public void otherThreadCannotReadOptimisticWrites() {
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

    /**
     * This test ensures if modifying multiple keys, with one key that does
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

    /**
     * Ensure that, upon two consecutive nested transactions, the latest transaction can
     * see optimistic updates from previous ones.
     */
    @Test
    public void OptimisticStreamGetUpdatedCorrectlyWithNestedTransaction() {
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

    /**
     * Threads that start a transaction at the same time
     * (with the same timestamp) should cause one thread
     * to abort while the other succeeds.
     */
    @Test
    public void threadShouldAbortAfterConflict() {
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

    /**
     * This test makes sure that nested transactions have same txn type
     * by checking that if nested transaction has a different type than
     * the parent transaction, an exception is thrown when the nested
     * transaction starts.
     */
    @Test
    public void nestedTransactionsHaveSameType() {
        t(1, this::OptimisticTXBegin);
        t(1, this::WWTXBegin).assertThrows().isInstanceOf(IllegalArgumentException.class);
        t(1, this::SnapshotTXBegin).assertThrows().isInstanceOf(IllegalArgumentException.class);
        t(1, this::TXEnd);
    }

    /**
     * This test makes sure that a single thread can read
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

    /**
     * This test makes sure that the nested transactions
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
    @Test
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

    /**
     * This test makes sure that a write-only transaction properly
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


    /**
     * This test checks if modifying two keys from
     * two different streams will cause a collision.
     * <p>
     * In the old single-level design, this would cause
     * a collision since 16 bits of the stream id were
     * being hashed into the 32 bit hashCode(), so that
     * certain stream-conflictkey combinations would
     * collide, as demonstrated below.
     * <p>
     * TODO: Potentially remove this unit test
     * TODO: once the hash function has stabilized.
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
        t2(() -> rt.getObjectsView().TXEnd())
                .assertDoesNotThrow(TransactionAbortedException.class);
    }

    /**
     * This test checks that transactions that span epochs can commit if the primary sequencer
     * does not change for those epochs (i.e. the sequencer spans a consecutive epochs range
     * from the transaction snapshot epoch to its current epoch).
     * <p>
     * t1 checks the transaction can commit if the sequencer has consecutive epochs.
     * t2 checks the transaction should abort if the sequencer does not have consecutive epochs.
     */
    @Test
    public void txnSpansEpochsCanCommitIfPrimarySequencerSpansConsecutiveEpochs() throws Exception {
        t1(this::OptimisticTXBegin);
        t2(this::OptimisticTXBegin);
        t1(() -> write("k1", "v1"));
        t2(() -> write("k2", "v2"));

        // Manually obtain snapshot timestamp before commit since it is evaluated lazily
        t1(() -> assertThat(TransactionalContext.getCurrentContext().getSnapshotTimestamp().getEpoch()).isEqualTo(0L));
        t2(() -> assertThat(TransactionalContext.getCurrentContext().getSnapshotTimestamp().getEpoch()).isEqualTo(0L));

        CorfuRuntime rt = getRuntime().connect();

        Layout layout = rt.getLayoutView().getLayout();
        Layout newLayout1 = new Layout(layout);
        newLayout1.nextEpoch();

        bootstrapAllServers(layout);

        // Move layout and sequencer epoch to 1 before commit
        rt.getLayoutView().getRuntimeLayout(newLayout1).sealMinServerSet();
        rt.getLayoutView().updateLayout(newLayout1, 1L);
        rt.getLayoutManagementView().reconfigureSequencerServers(layout, newLayout1, false);

        assertThat(getSequencer(SERVERS.PORT_0).getSequencerEpoch()).isEqualTo(newLayout1.getEpoch());
        assertThat(getSequencer(SERVERS.PORT_0).getEpochRangeLowerBound()).isEqualTo(layout.getEpoch());

        // When t1 wants to commit, the sequencer has consecutive epochs, so it should commit successfully.
        t1(this::TXEnd).assertDoesNotThrow(TransactionAbortedException.class);

        Layout newLayout2 = new Layout(newLayout1);
        newLayout2.nextEpoch();

        // Move layout to epoch 3
        rt.getLayoutView().getRuntimeLayout(newLayout2).sealMinServerSet();
        rt.getLayoutView().updateLayout(newLayout2, 1L);
        rt.invalidateLayout();

        newLayout2.nextEpoch();
        rt.getLayoutView().getRuntimeLayout(newLayout2).sealMinServerSet();
        rt.getLayoutView().updateLayout(newLayout2, 1L);

        // Move sequencer epoch to 3 so that it does not have consecutive epochs (lost epoch 2)
        rt.getLayoutManagementView().reconfigureSequencerServers(newLayout1, newLayout2, true);

        assertThat(getSequencer(SERVERS.PORT_0).getSequencerEpoch()).isEqualTo(newLayout2.getEpoch());
        assertThat(getSequencer(SERVERS.PORT_0).getEpochRangeLowerBound()).isEqualTo(newLayout2.getEpoch());

        // When t2 wants to commit, the sequencer does not have consecutive epochs, so it should abort.
        t2(this::TXEnd).assertThrows().isInstanceOf(TransactionAbortedException.class);
    }

    /**
     * Test that the transaction can commit if the first attempt got overwritten and retried.
     * Basically on overwrite and retry we need to adjust the transaction snapshot otherwise
     * the transaction will always conflict with itself and get TransactionAbortedException.
     */
    @Test
    public void txnCanCommitIfOverwrittenAndRetry() throws Exception {
        // Get a new runtime.
        CorfuRuntime rt = getNewRuntime(getRuntime().getParameters())
                .parseConfigurationString(getDefaultConfigurationString());
        rt.connect();

        OptimisticTXBegin();

        put("k1", "v1");
        put("k2", "v2");

        // Hole fill the address that the transaction is about to commit, to
        // create an OverwriteException and retry. The retry should succeed.
        long currentTail = rt.getSequencerView().query().getToken().getSequence();
        rt.getAddressSpaceView().read(currentTail + 1);

        assertThatCode(this::TXEnd).doesNotThrowAnyException();

        assertThat(rt.getSequencerView().query().getToken().getSequence()).isEqualTo(currentTail + 2);
        assertThat(get("k1")).isEqualTo("v1");
        assertThat(get("k2")).isEqualTo("v2");
    }
}
