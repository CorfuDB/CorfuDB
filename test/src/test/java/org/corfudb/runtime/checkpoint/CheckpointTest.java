package org.corfudb.runtime.checkpoint;

import com.google.common.collect.Iterators;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    Map<String, Long> openMap(CorfuRuntime rt, String mapName) {
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        rt.getSerializers().registerSerializer(serializer);
        return (CorfuTable<String, Long>)
                instantiateCorfuObject(
                        rt,
                        new TypeToken<CorfuTable<String, Long>>() {
                        },
                        mapName,
                        serializer);
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
     * checkpoint the maps
     */
    void mapCkpoint(CorfuRuntime rt, CorfuTable... maps) throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            for (CorfuTable map : maps) {
                mcw1.addMap(map);
            }

            mcw1.appendCheckpoints(rt, author);
        }
    }

    volatile Token lastValidCheckpoint = Token.UNINITIALIZED;

    /**
     * checkpoint the maps, and then trim the log
     */
    void mapCkpointAndTrim(CorfuRuntime rt, CorfuTable... maps) throws Exception {
        Token checkpointAddress = Token.UNINITIALIZED;
        Token lastCheckpointAddress = Token.UNINITIALIZED;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            try {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                for (CorfuTable map : maps) {
                    mcw1.addMap(map);
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
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSize, boolean expectedFullsize, boolean trimCanHappen) {
        CorfuRuntime rt = getNewRuntime();
        try {
            Map<String, Long> localm2A = openMap(rt, streamNameA);
            Map<String, Long> localm2B = openMap(rt, streamNameB);
            for (int i = 0; i < localm2A.size(); i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo(i);
            }
            for (int i = 0; i < localm2B.size(); i++) {
                assertThat(localm2B.get(String.valueOf(i))).isEqualTo(0L);
            }
            if (expectedFullsize) {
                assertThat(localm2A.size()).isEqualTo(mapSize);
                assertThat(localm2B.size()).isEqualTo(mapSize);
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
     * initialize the two maps, the second one is all zeros
     *
     * @param mapSize
     */
    void populateMaps(int mapSize, Map<String, Long> map1, Map<String, Long> map2) {
        for (int i = 0; i < mapSize; i++) {
            try {
                map1.put(String.valueOf(i), (long) i);
                map2.put(String.valueOf(i), (long) 0);
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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        CorfuRuntime rt = getNewRuntime();

        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(mapSize, mapA, mapB);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (CorfuTable) mapA, (CorfuTable) mapB);
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, false, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true, false);

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
        final int mapSize = 0;

        CorfuRuntime rt = getNewRuntime();
        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (CorfuTable) mapA, (CorfuTable) mapB);
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, true, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after the two threads finish, again we start a fresh runtime and instantiate the maps.
        // Then verify they are empty.
        validateMapRebuild(mapSize, true, false);

        rt.shutdown();
    }

    @Test
    public void emptyCkpointTest2() throws Exception {
        final int mapSize = 0;

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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;

        CorfuRuntime rt = getNewRuntime();
        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        // pre-populate map
        populateMaps(mapSize, mapA, mapB);

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (CorfuTable) mapA, (CorfuTable) mapB);
        });

        // repeated ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // this thread checks that all values are present in the maps
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(mapSize, true, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(mapSize, true, false);

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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int WAIT_TIME_SECONDS = 10;

        CorfuRuntime rt = getNewRuntime();

        try {
            Map<String, Long> mapA = openMap(rt, streamNameA);
            Map<String, Long> mapB = openMap(rt, streamNameB);

            // thread 1: populates the maps with mapSize items
            scheduleConcurrently(1, ignored_task_num -> {
                populateMaps(mapSize, mapA, mapB);
            });

            // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
            // and immediate prefix-trim of the log up to the checkpoint position
            scheduleConcurrently(1, ignored_task_num -> {
                mapCkpointAndTrim(rt, (CorfuTable) mapA, (CorfuTable) mapB);
            });

            // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
            // they should rebuild from the latest checkpoint (if available).
            // performs some sanity checks on the map state
            scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
                validateMapRebuild(mapSize, false, true);
            });

            executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

            // Verify that last trim cycle completed (async task) before validating map rebuild
            Token currentTrimMark =  Token.UNINITIALIZED;
            while(currentTrimMark.getSequence() != lastValidCheckpoint.getSequence() + 1L) {
                currentTrimMark = getRuntime().getAddressSpaceView().getTrimMark();
            }

            // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
            // This time we check that the new map instances contain all values
            validateMapRebuild(mapSize, true, false);
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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        CorfuRuntime rt = getNewRuntime();

        TestRule dropSequencerTrim = new TestRule()
                .requestMatches(msg -> msg.getPayload().getPayloadCase()
                        .equals(CorfuMessage.RequestPayloadMsg.PayloadCase.SEQUENCER_TRIM_REQUEST))
                .drop();
        addClientRule(rt, dropSequencerTrim);

        try {
            Map<String, Long> mapA = openMap(rt, streamNameA);
            Map<String, Long> mapB = openMap(rt, streamNameB);

            // thread 1: populates the maps with mapSize items
            scheduleConcurrently(1, ignored_task_num -> {
                populateMaps(mapSize, mapA, mapB);
            });

            // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
            // and immediate prefix-trim of the log up to the checkpoint position
            scheduleConcurrently(1, ignored_task_num -> {
                mapCkpointAndTrim(rt, (CorfuTable) mapA, (CorfuTable) mapB);
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
                validateMapRebuild(mapSize, false, true);
            });

            executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

            // Verify that last trim cycle completed (async task) before validating map rebuild
            Token currentTrimMark =  Token.UNINITIALIZED;
            while(currentTrimMark.getSequence() != lastValidCheckpoint.getSequence() + 1L) {
                currentTrimMark = getNewRuntime().getAddressSpaceView().getTrimMark();
            }

            // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
            // This time we check that the new map instances contain all values
            validateMapRebuild(mapSize, true, false);
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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final int trimPosition = mapSize / 2;
        final int snapshotPosition = trimPosition + 2;

        CountDownLatch latch = new CountDownLatch(1);

        t(1, () -> {

            CorfuRuntime rt = getNewRuntime();
            try {
                Map<String, Long> mapA = openMap(rt, streamNameA);

                // first, populate the map
                for (int i = 0; i < mapSize; i++) {
                    mapA.put(String.valueOf(i), (long) i);
                }

                // now, take a checkpoint and perform a prefix-trim
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((CorfuTable) mapA);
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

            Map<String, Long> localm2A = openMap(rt, streamNameA);

            // start a snapshot TX at position snapshotPosition
            Token timestamp = new Token(0L, snapshotPosition - 1);
            rt.getObjectsView().TXBuild()
                    .type(TransactionType.SNAPSHOT)
                    .snapshot(timestamp)
                    .build()
                    .begin();

            // finally, instantiate the map for the snapshot and assert is has the right state
            try {
                localm2A.get(0);
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
                    for (int i = snapshotPosition; i < mapSize; i++) {
                        assertThat(localm2A.get(String.valueOf(i)))
                                .isEqualTo(null);
                    }
                }
            } finally {
                rt.shutdown();
            }
        });

    }
    
    @Test
    public void prefixTrimTwiceAtSameAddress() throws Exception {
        final int mapSize = 5;
        CorfuRuntime rt = getNewRuntime();
        Map<String, Long> map1 = openMap(rt, streamNameA);
        Map<String, Long> map2 = openMap(rt, streamNameB);
        populateMaps(mapSize, map1, map2);

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
        Map<String, String> testMap = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test")
                .open();

        Map<String, String> testMap2 = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                })
                .setStreamName("test2")
                .open();

        // Place entries into the map
        testMap.put("a", "a");
        testMap.put("b", "a");
        testMap.put("c", "a");
        testMap2.put("a", "c");
        testMap2.put("b", "c");
        testMap2.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap((CorfuTable) testMap);
        mcw.addMap((CorfuTable) testMap2);
        Token checkpointAddress = mcw.appendCheckpoints(getRuntime(), "author");

        // TX1: Move object to 1
        Token timestamp = new Token(0L, 1);
        getRuntime().getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .snapshot(timestamp)
                .build()
                .begin();

        testMap.get("a");
        getRuntime().getObjectsView().TXEnd();

        // Trim the log
        getRuntime().getAddressSpaceView().prefixTrim(checkpointAddress);
        getRuntime().getAddressSpaceView().gc();
        getRuntime().getAddressSpaceView().invalidateServerCaches();
        getRuntime().getAddressSpaceView().invalidateClientCache();

        // TX2: Read most recent state in TX
        getRuntime().getObjectsView().TXBegin();
        testMap.put("a", "b");
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
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        final String streamName = "test";

        CorfuRuntime rt = getNewRuntime();
        CorfuRuntime runtime = getNewRuntime();

        try {
            StreamingMap<String, Long> testMap = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, Long>>() {
                    })
                    .setStreamName(streamName)
                    .open();

            // Checkpoint (should return -1 as no  actual data is written yet)
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addMap(testMap);
            Token checkpointAddress = mcw.appendCheckpoints(rt, author);

            // Write several entries into the log.
            for (int i = 0; i < mapSize; i++) {
                try {
                    testMap.put(String.valueOf(i), (long) i);
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
                Map<String, Long> localTestMap = openMap(runtime, streamName);
                for (int i = 0; i < localTestMap.size(); i++) {
                    assertThat(localTestMap.get(String.valueOf(i))).isEqualTo(i);
                }
                assertThat(localTestMap.size()).isEqualTo(mapSize);
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
    public void PersistedCorfuTableTests() {
        String path = PARAMETERS.TEST_TEMP_DIR;

        CorfuRuntime rt = getDefaultRuntime();

        UUID tableId = UUID.randomUUID();
        Supplier<StreamingMap<String, String>> writerMapSupplier = () ->
                new PersistedStreamingMap<>(Paths.get(path + tableId + "writer"),
                        PersistedStreamingMap.getPersistedStreamingMapOptions(),
                        Serializers.JSON, rt);

        CorfuTable<String, String> diskBackedMap = rt.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .streamID(tableId)
                .setSerializer(Serializers.JSON)
                .setArguments(writerMapSupplier, ICorfuVersionPolicy.MONOTONIC)
                .open();

        final int numKeys = 303;
        for (int x = 0; x < numKeys; x++) {
            diskBackedMap.put(String.valueOf(x), "payload" + x);
        }

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(diskBackedMap);
        Token token = mcw.appendCheckpoints(rt, "Author1");
        rt.getAddressSpaceView().prefixTrim(token);
        rt.getAddressSpaceView().gc();

        CorfuRuntime newRt = getNewRuntime();

        Supplier<StreamingMap<String, String>> readerMapSupplier = () ->
                new PersistedStreamingMap<>(Paths.get(path + tableId + "reader"),
                        PersistedStreamingMap.getPersistedStreamingMapOptions(),
                        Serializers.JSON, rt);

        CorfuTable<String, String> newDiskBackedMap = newRt.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .streamID(tableId)
                .setSerializer(Serializers.JSON)
                .setArguments(readerMapSupplier, ICorfuVersionPolicy.MONOTONIC)
                .open();

        assertThat(Iterators.elementsEqual(newDiskBackedMap.entryStream().iterator(),
                diskBackedMap.entryStream().iterator())).isTrue();
    }
}
