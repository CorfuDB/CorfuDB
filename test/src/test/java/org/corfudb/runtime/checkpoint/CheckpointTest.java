package org.corfudb.runtime.checkpoint;

import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.Sleep;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 5/25/17.
 */
@SuppressWarnings("checkstyle:magicnumber")
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    PersistentCorfuTable<String, Long> openTable(CorfuRuntime rt, String tableName) {
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        rt.getSerializers().registerSerializer(serializer);
        return rt.getObjectsView()
                .build()
                .setStreamName(tableName)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, Long>>() {})
                .setSerializer(serializer)
                .open();
    }

    @Before
    public void instantiateMaps() {
        getDefaultRuntime();
    }

    final String streamNameA = "mystreamA";
    final String streamNameB = "mystreamB";
    final String author = "ckpointTest";

    private CorfuRuntime getNewRuntime() {
        return getNewRuntime(getDefaultNode()).connect();
    }

    /**
     * checkpoint the tables
     */
    void mapCkpoint(CorfuRuntime rt, PersistentCorfuTable<?, ?>... tables) throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw1 = new MultiCheckpointWriter<>();
            for (PersistentCorfuTable<?, ?> table : tables) {
                mcw1.addMap(table);
            }

            mcw1.appendCheckpoints(rt, author);
        }
    }

    volatile Token lastValidCheckpoint = Token.UNINITIALIZED;

    /**
     * checkpoint the tables, and then trim the log
     */
    void mapCkpointAndTrim(CorfuRuntime rt, PersistentCorfuTable<?, ?>... tables) throws Exception {
        Token checkpointAddress = Token.UNINITIALIZED;
        Token lastCheckpointAddress = Token.UNINITIALIZED;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            try {
                MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw1 = new MultiCheckpointWriter<>();
                for (PersistentCorfuTable<?, ?> table : tables) {
                    mcw1.addMap(table);
                }

                checkpointAddress = mcw1.appendCheckpoints(rt, author);

                try {
                    Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                } catch (InterruptedException ie) {
                    //
                }
                // Trim the log
                rt.getAddressSpaceView().prefixTrim(checkpointAddress);
                rt.getAddressSpaceView().gc();
                rt.getAddressSpaceView().invalidateServerCaches();
                rt.getAddressSpaceView().invalidateClientCache();
                lastCheckpointAddress = checkpointAddress;
                lastValidCheckpoint = checkpointAddress;
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            } catch (Exception e) {
                // We are the only thread performing trimming, therefore any
                // prior trim must have been by our own action.  We assume
                // that we don't go backward, so checking for equality will
                // catch violations of that assumption.
                assertThat(checkpointAddress).isEqualTo(lastCheckpointAddress);
            }
        }
    }

    /**
     * Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     *
     * @param tableSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int tableSize, boolean expectedFullsize, boolean trimCanHappen) {
        CorfuRuntime rt = getNewRuntime();
        try {
            PersistentCorfuTable<String, Long> localm2A = openTable(rt, streamNameA);
            PersistentCorfuTable<String, Long> localm2B = openTable(rt, streamNameB);
            for (int i = 0; i < localm2A.size(); i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo(i);
            }
            for (int i = 0; i < localm2B.size(); i++) {
                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
            }
            if (expectedFullsize) {
                assertThat(localm2A.size()).isEqualTo(tableSize);
                assertThat(localm2B.size()).isEqualTo(tableSize);
            }
        } catch (TrimmedException te) {
            if (!trimCanHappen) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }
        } finally {
            rt.shutdown();
        }
    }

    /**
     * initialize the two tables, the second one is all zeros
     *
     * @param tableSize
     */
    void populateMaps(int tableSize, PersistentCorfuTable<String, Long> table1, PersistentCorfuTable<String, Long> table2) {
        for (int i = 0; i < tableSize; i++) {
            try {
                table1.insert(String.valueOf(i), (long) i);
                table2.insert(String.valueOf(i), (long) 0);
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }
        }
    }

    /**
     * this test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one populates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 3. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointTest() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;
        CorfuRuntime rt = getNewRuntime();

        PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);
        PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

        // thread 1: populates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(tableSize, tableA, tableB);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, tableA, tableB);
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(tableSize, false, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(tableSize, true, false);

        rt.shutdown();
    }

    /**
     * this test builds two maps, m2A m2B, and brings up two threads:
     * <p>
     * 1. one thread performs ITERATIONS_VERY_LOW checkpoints
     * 2. one thread repeats ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
     * they should be empty.
     * <p>
     * Finally, after the two threads finish, again we start a fresh runtime and instantiate the maps.
     * Then verify they are empty.
     *
     * @throws Exception
     */
    @Test
    public void emptyCkpointTest() throws Exception {
        final int tableSize = 0;

        CorfuRuntime rt = getNewRuntime();
        PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);
        PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, tableA, tableB);
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(tableSize, true, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after the two threads finish, again we start a fresh runtime and instantiate the maps.
        // Then verify they are empty.
        validateMapRebuild(tableSize, true, false);

        rt.shutdown();
    }

    @Test
    public void emptyCkpointMVOTest() throws Exception {

        CorfuRuntime rt = getNewRuntime();
        PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);

        tableA.insert("a", 0L);
        tableA.delete("a");

        // Checkpoint
        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw1 = new MultiCheckpointWriter<>();
        mcw1.addMap(tableA);
        Token trimToken = mcw1.appendCheckpoints(rt, author);

        rt.getAddressSpaceView().prefixTrim(trimToken);
        rt.shutdown();

        // Start a new runtime otherwise the old runtime will not load the CP
        // entries since the read queue has already resolved the updates
        rt = getNewRuntime();
        tableA = openTable(rt, streamNameA);

        tableA.insert("c", 1L);
        assertThat(tableA.size()).isEqualTo(1);

        // Access should be served with version 2
        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(new Token(0, 4))
                .build()
                .begin();
        assertThat(tableA.size()).isZero();
        rt.getObjectsView().TXEnd();

        rt.shutdown();
    }

    @Test
    public void emptyCkpointTest2() throws Exception {
        CorfuRuntime rt = getNewRuntime();
        rt.shutdown();
    }

    /**
     * this test is similar to periodicCkpointTest(), but populating the maps is done BEFORE starting the checkpoint/recovery threads.
     * <p>
     * First, the test builds two maps, m2A m2B, and populates them with mapSize items.
     * <p>
     * Then, it brings up two threads:
     * <p>
     * 1. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 2. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread checks that all values are present in the maps
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointNoUpdatesTest() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;

        CorfuRuntime rt = getNewRuntime();
        PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);
        PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

        // pre-populate map
        populateMaps(tableSize, tableA, tableB);

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, tableA, tableB);
        });

        // repeated ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // this thread checks that all values are present in the maps
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(tableSize, true, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
        // This time we check that the new map instances contains all values
        validateMapRebuild(tableSize, true, false);

        rt.shutdown();
    }

    /**
     * this test is similar to periodicCkpointTest(), but adds simultaneous log prefix-trimming.
     * <p>
     * the test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one populates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
     * and immediately trims the log up to the checkpoint position.
     * 3. one repeats ITERATIONS_LOW starting a fresh runtime, and instantiating the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */

    @Test
    public void periodicCkpointTrimTest() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;
        CorfuRuntime rt = getNewRuntime();

        try {
            PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);
            PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

            // thread 1: populates the maps with mapSize items
            scheduleConcurrently(1, ignored_task_num -> {
                populateMaps(tableSize, tableA, tableB);
            });

            // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
            // and immediate prefix-trim of the log up to the checkpoint position
            scheduleConcurrently(1, ignored_task_num -> {
                mapCkpointAndTrim(rt, tableA, tableB);
            });

            // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
            // they should rebuild from the latest checkpoint (if available).
            // performs some sanity checks on the map state
            scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
                validateMapRebuild(tableSize, false, true);
            });

            executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

            // Verify that last trim cycle completed (async task) before validating map rebuild
            Token currentTrimMark =  Token.UNINITIALIZED;
            while(currentTrimMark.getSequence() != lastValidCheckpoint.getSequence() + 1L) {
                currentTrimMark = getRuntime().getAddressSpaceView().getTrimMark();
            }

            // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
            // This time we check that the new map instances contain all values
            validateMapRebuild(tableSize, true, false);
        } finally {
            rt.shutdown();
        }
    }

    /**
     * This test verifies that a stream can be rebuilt from a checkpoint even
     * when the prefix trim message has not reached the sequencer but yet addresses
     * have been actually trimmed from the log.
     *
     * @throws Exception
     */
    @Test
    public void prefixTrimNotReachingSequencer() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;
        CorfuRuntime rt = getNewRuntime();

        TestRule dropSequencerTrim = new TestRule()
                .requestMatches(msg -> msg.getPayload().getPayloadCase()
                        .equals(CorfuMessage.RequestPayloadMsg.PayloadCase.SEQUENCER_TRIM_REQUEST))
                .drop();
        addClientRule(rt, dropSequencerTrim);

        try {
            PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);
            PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

            // thread 1: populates the maps with mapSize items
            scheduleConcurrently(1, ignored_task_num -> {
                populateMaps(tableSize, tableA, tableB);
            });

            // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
            // and immediate prefix-trim of the log up to the checkpoint position
            scheduleConcurrently(1, ignored_task_num -> {
                mapCkpointAndTrim(rt, tableA, tableB);
            });

            // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
            // they should rebuild from the latest checkpoint (if available).
            // performs some sanity checks on the map state

            // In this test checkpoints and trims are happening in a loop for several iterations,
            // so a trim exception can occur if after loading from the latest checkpoint,
            // updates to the stream have been already trimmed (1st trimmedException), the stream
            // is reset, and on the second iteration the same scenario can happen (2nd trimmedException)
            // which is the total number of retries.
            scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
                validateMapRebuild(tableSize, false, true);
            });

            executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

            // Verify that last trim cycle completed (async task) before validating map rebuild
            Token currentTrimMark =  Token.UNINITIALIZED;
            while(currentTrimMark.getSequence() != lastValidCheckpoint.getSequence() + 1L) {
                currentTrimMark = getNewRuntime().getAddressSpaceView().getTrimMark();
            }

            // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
            // This time we check that the new map instances contain all values
            validateMapRebuild(tableSize, true, false);
        } finally {
            rt.shutdown();
        }
    }

    /**
     * This test verifies that a client that recovers a map from checkpoint,
     * but wants the map at a snapshot -earlier- than the snapshot,
     * will either get a transactionAbortException, or get the right version of
     * the map.
     * <p>
     * It works as follows.
     * We build a map with one hundred entries [0, 1, 2, 3, ...., 99].
     * <p>
     * Note that, each entry is one put, so if a TX starts at snapshot at 77, it should see a map with 77 items 0, 1, 2, ..., 76.
     * We are going to verify that this works even if we checkpoint the map, and trim a prefix, say of the first 50 put's.
     * <p>
     * First, we then take a checkpoint of the map.
     * <p>
     * Then, we prefix-trim the log up to position 50.
     * <p>
     * Now, we start a new runtime and instantiate this map. It should build the map from a checkpoint.
     * <p>
     * Finally, we start a snapshot-TX at timestamp 77. We verify that the map state is [0, 1, 2, 3, ..., 76].
     */
    @Test
    public void undoCkpointTest() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int trimPosition = tableSize / 2;
        final int snapshotPosition = trimPosition + 2;

        CountDownLatch latch = new CountDownLatch(1);

        t(1, () -> {

            CorfuRuntime rt = getNewRuntime();
            try {
                PersistentCorfuTable<String, Long> tableA = openTable(rt, streamNameA);

                // first, populate the map
                for (int i = 0; i < tableSize; i++) {
                    tableA.insert(String.valueOf(i), (long) i);
                }

                // now, take a checkpoint and perform a prefix-trim
                MultiCheckpointWriter<PersistentCorfuTable<String, Long>> mcw1 = new MultiCheckpointWriter<>();
                mcw1.addMap(tableA);
                mcw1.appendCheckpoints(rt, author);

                // Trim the log
                Token token = new Token(rt.getLayoutView().getLayout().getEpoch(), trimPosition);
                rt.getAddressSpaceView().prefixTrim(token);
                rt.getAddressSpaceView().gc();
                rt.getAddressSpaceView().invalidateServerCaches();
                rt.getAddressSpaceView().invalidateClientCache();

                latch.countDown();
            } finally {
                rt.shutdown();
            }
        });

        AtomicBoolean trimExceptionFlag = new AtomicBoolean(false);

        // start a new runtime
        t(2, () -> {
            latch.await();
            CorfuRuntime rt = getNewRuntime();

            PersistentCorfuTable<String, Long> localm2A = openTable(rt, streamNameA);

            // start a snapshot TX at position snapshotPosition
            Token timestamp = new Token(0L, snapshotPosition - 1);
            rt.getObjectsView().TXBuild()
                    .type(TransactionType.SNAPSHOT)
                    .snapshot(timestamp)
                    .build()
                    .begin();

            // finally, instantiate the map for the snapshot and assert is has the right state
            try {
                localm2A.get(String.valueOf(0));
            } catch (TransactionAbortedException te) {
                assertThat(te.getAbortCause()).isEqualTo(AbortCause.TRIM);
                // this is an expected behavior!
                trimExceptionFlag.set(true);
            }

            try {
                if (!trimExceptionFlag.get()) {
                    assertThat(localm2A.size())
                            .isEqualTo(snapshotPosition);

                    // check map positions 0..(snapshot-1)
                    for (int i = 0; i < snapshotPosition; i++) {
                        assertThat(localm2A.get(String.valueOf(i)))
                                .isEqualTo(i);
                    }

                    // check map positions snapshot..(mapSize-1)
                    for (int i = snapshotPosition; i < tableSize; i++) {
                        assertThat(localm2A.get(String.valueOf(i))).isNull();
                    }
                }
            } finally {
                rt.shutdown();
            }
        });
    }
    
    @Test
    public void prefixTrimTwiceAtSameAddress() throws Exception {
        final int tableSize = 5;
        CorfuRuntime rt = getNewRuntime();
        PersistentCorfuTable<String, Long> table1 = openTable(rt, streamNameA);
        PersistentCorfuTable<String, Long> table2 = openTable(rt, streamNameB);
        populateMaps(tableSize, table1, table2);

        // Trim again in exactly the same place shouldn't fail
        Token token = new Token(rt.getLayoutView().getLayout().getEpoch(), 2);
        rt.getAddressSpaceView().prefixTrim(token);
        rt.getAddressSpaceView().prefixTrim(token);

        // GC twice at the same place should be fine.
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().gc();
        rt.shutdown();
    }

    @Test
    public void transactionalReadAfterCheckpoint() throws Exception {
        PersistentCorfuTable<String, String> testTable = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test")
                .open();

        PersistentCorfuTable<String, String> testTable2 = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName("test2")
                .open();

        // Place entries into the map
        testTable.insert("a", "a");
        testTable.insert("b", "a");
        testTable.insert("c", "a");
        testTable2.insert("a", "c");
        testTable2.insert("b", "c");
        testTable2.insert("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter<PersistentCorfuTable<String, String>> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(testTable);
        mcw.addMap(testTable2);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // TX1: Move object to 1
        Token timestamp = new Token(0L, 1);
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(timestamp)
                .build()
                .begin();

        assertThat(testTable.get("a")).isEqualTo("a");
        getRuntime().getObjectsView().TXEnd();

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // TX2: Read most recent state in TX
        getRuntime().getObjectsView().TXBegin();
        assertThat(testTable.get("a")).isEqualTo("a");
        testTable.insert("a", "b");
        assertThat(testTable.get("a")).isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
    }

    /**
     * This test validates that trimming the address space on a non-existing address (-1)
     * after data is already present in the log, does not lead to sequencer trims.
     *
     * Note: this comes along with the fact that trimming the bitmap on a negative value,
     * gives the cardinality of infinity (all entries in the map).
     *
     */
    @Test
    public void testPrefixTrimOnNonAddress() throws Exception {
        final int tableSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final String streamName = "test";

        CorfuRuntime rt = getNewRuntime();
        CorfuRuntime runtime = getNewRuntime();

        try {
            PersistentCorfuTable<String, Long> testTable = rt.getObjectsView()
                    .build()
                    .setTypeToken(new TypeToken<PersistentCorfuTable<String, Long>>() {})
                    .setStreamName(streamName)
                    .open();

            // Checkpoint (should return -1 as no  actual data is written yet)
            MultiCheckpointWriter<PersistentCorfuTable<String, Long>> mcw = new MultiCheckpointWriter<>();
            mcw.addMap(testTable);
            Token checkpointAddress = mcw.appendCheckpoints(rt, author);

            // Write several entries into the log.
            for (int i = 0; i < tableSize; i++) {
                try {
                    testTable.insert(String.valueOf(i), (long) i);
                } catch (TrimmedException te) {
                    // shouldn't happen
                    te.printStackTrace();
                    throw te;
                }
            }

            // Prefix Trim on checkpoint snapshot address
            rt.getAddressSpaceView().prefixTrim(checkpointAddress);
            rt.getAddressSpaceView().gc();
            rt.getAddressSpaceView().invalidateServerCaches();
            rt.getAddressSpaceView().invalidateClientCache();

            // Rebuild stream from fresh runtime
            try {
                PersistentCorfuTable<String, Long> localTestTable = openTable(runtime, streamName);
                for (int i = 0; i < localTestTable.size(); i++) {
                    assertThat(localTestTable.get(String.valueOf(i))).isEqualTo(i);
                }
                assertThat(localTestTable.size()).isEqualTo(tableSize);
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }
        } finally {
            rt.shutdown();
            runtime.shutdown();
        }
    }

    /**
     * This test creates a CorfuTable backed by a RocksDb can be checkpointed.
     */

    @Test
    public void persistedCorfuTableCheckpointTest() {
        String path = PARAMETERS.TEST_TEMP_DIR;

        CorfuRuntime rt = getDefaultRuntime();

        final UUID tableId = UUID.randomUUID();
        final PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(path + tableId + "writer"));
        final int numKeys = 303;

        try (PersistedCorfuTable<String, String> table = rt.getObjectsView()
                .build()
                .setTypeToken(PersistedCorfuTable.<String, String>getTypeToken())
                .setStreamID(tableId)
                .setSerializer(Serializers.JSON)
                .setArguments(persistenceOptions.build(), Serializers.JSON)
                .open()) {

            for (int x = 0; x < numKeys; x++) {
                table.insert(String.valueOf(x), "payload" + x);
            }
            assertThat(table.size()).isEqualTo(numKeys);

            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addMap(table);
            Token token = mcw.appendCheckpoints(rt, "Author1");
            rt.getAddressSpaceView().prefixTrim(token);
            rt.getAddressSpaceView().gc();
        }

        CorfuRuntime newRt = getNewRuntime();

        persistenceOptions.dataPath(Paths.get(path + tableId + "reader"));
        try (PersistedCorfuTable<String, String> table = newRt.getObjectsView().build()
                .setTypeToken(PersistedCorfuTable.<String, String>getTypeToken())
                .setStreamID(tableId)
                .setSerializer(Serializers.JSON)
                .setArguments(persistenceOptions.build(), Serializers.JSON)
                .open()) {

            assertThat(Iterators.elementsEqual(table.entryStream().iterator(),
                    table.entryStream().iterator())).isTrue();
            assertThat(table.size()).isEqualTo(numKeys);

        }
    }

    /**
     * Test that validates that a DiskBacked CorfuTable can correctly serve snapshots
     * after encountering a TrimmedException from the stream layer, even in the presence
     * of EXPIRED snapshots in the MVOCache.
     */
    @Test
    public void persistedCorfuTableCheckpointTrimTest() {
        final String path = PARAMETERS.TEST_TEMP_DIR;
        final int mvoCacheExpirySeconds = 1;

        CorfuRuntime rt = getDefaultRuntime();
        rt.getParameters().setMvoCacheExpiry(Duration.ofSeconds(mvoCacheExpirySeconds));

        final UUID tableId = UUID.randomUUID();
        final PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(path + tableId + "reader"));

        try (PersistedCorfuTable<String, String> tableRef = rt.getObjectsView()
                .build()
                .setTypeToken(PersistedCorfuTable.<String, String>getTypeToken())
                .setStreamID(tableId)
                .setSerializer(Serializers.JSON)
                .setArguments(persistenceOptions.build(), Serializers.JSON)
                .open()) {

            // Populate initial data in the CorfuTable.
            final int numKeys = 100;
            for (int i = 0; i < numKeys; i++) {
                tableRef.insert(Integer.toString(i), Integer.toString(i));
            }

            // Perform a read and populate the MVOCache.
            assertThat(tableRef.size()).isEqualTo(numKeys);

            CorfuRuntime cpRt = getNewRuntime();
            persistenceOptions.dataPath(Paths.get(path + tableId + "cpWriter"));

            // Perform Checkpoint/Trim cycles to induce a TrimmedException from
            // the streaming layer in the first runtime during the next read.
            try (PersistedCorfuTable<String, String> tableRef2 = cpRt.getObjectsView().build()
                    .setTypeToken(PersistedCorfuTable.<String, String>getTypeToken())
                    .setStreamID(tableId)
                    .setSerializer(Serializers.JSON)
                    .setArguments(persistenceOptions.build(), Serializers.JSON)
                    .open()) {

                for (int y = 0; y < 3; y++) {
                    MultiCheckpointWriter mcw = new MultiCheckpointWriter();
                    mcw.addMap(tableRef2);
                    Token token = mcw.appendCheckpoints(cpRt, "cp");
                    cpRt.getAddressSpaceView().prefixTrim(token);
                    cpRt.getAddressSpaceView().gc();
                }
            }

            cpRt.shutdown();

            // Allow enough time so that existing snapshots in the MVOCache become EXPIRED.
            Sleep.sleepUninterruptibly(Duration.ofSeconds(2*mvoCacheExpirySeconds));

            // Validate that table operations can still be performed correctly without crashing/exceptions.
            assertThat(tableRef.size()).isEqualTo(numKeys);
            rt.shutdown();
        }
    }
}
