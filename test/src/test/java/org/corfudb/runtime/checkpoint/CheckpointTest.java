package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    @Getter
    CorfuRuntime myRuntime = null;

    void setRuntime() {
        myRuntime = new CorfuRuntime(getDefaultConfigurationString()).connect();
    }

    Map<String, Long> instantiateMap(String mapName) {
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        getMyRuntime(),
                        new TypeToken<SMRMap<String, Long>>() {},
                        mapName);
    }

    /**
     * this test builds two maps, m2A m2B, and brings up three threads:
     *
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 3. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     *    they should rebuild from the latest checkpoint (if available).
     *    this thread performs some sanity checks on the map state
     *
     *    Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     *    This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < mapSize; i++) {
                m2A.put(String.valueOf(i), (long)i);
                m2B.put(String.valueOf(i), (long)0);
            }
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                long firstGlobalAddress1 = mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)) == null ||
                        localm2A.get(String.valueOf(i)) == (long) i
                ).isTrue();
                assertThat(localm2B.get(String.valueOf(i)) == null ||
                        localm2B.get(String.valueOf(i)) == (long) 0
                ).isTrue();
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
        }

    }

    /**
     * this test builds two maps, m2A m2B, and brings up two threads:
     *
     * 1. one thread performs ITERATIONS_VERY_LOW checkpoints
     * 2. one thread repeats ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
     *    they should be empty.
     *
     *    Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
     *    Then verify they are empty.
     *
     * @throws Exception
     */
    @Test
    public void emptyCkpointTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        // thread 1: perform ITERATIONS_VERY_LOW checkpoints
        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                long firstGlobalAddress1 = mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        // thread 2: repeat ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should be empty.
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)) ).isNull();
                assertThat(localm2B.get(String.valueOf(i)) ).isNull();
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);


        // Finally, after the two threads finish, again we start a fresh runtime and instante the maps.
        // Then verify they are empty.
        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isNull();
            assertThat(localm2B.get(String.valueOf(i)) ).isNull();
        }

    }

    /**
     * this test is similar to periodicCkpointTest(), but populating the maps is done BEFORE starting the checkpoint/recovery threads.
     *
     * First, the test builds two maps, m2A m2B, and populates them with mapSize items.
     *
     * Then, it brings up two threads:
     *
     * 1. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
     * 2. one repeatedly (LOW times) starts a fresh runtime, and instantiates the maps.
     *    they should rebuild from the latest checkpoint (if available).
     *    this thread checks that all values are present in the maps
     *
     *    Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     *    This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */
    @Test
    public void periodicCkpointTestNoUpdates() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        // pre-populate map
        for (int i = 0; i < mapSize; i++) {
            m2A.put(String.valueOf(i), (long)i);
            m2B.put(String.valueOf(i), (long)0);
        }

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                mcw1.appendCheckpoints(currentRuntime, author);
            }
        });

        // repeated ITERATIONS_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // this thread checks that all values are present in the maps
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            for (int i = 0; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
                assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo((long) 0);
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
        }

    }

    /**
     * this test is similar to periodicCkpointTest(), but adds simultaneous log prefix-trimming.
     *
     * the test builds two maps, m2A m2B, and brings up three threads:
     *
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
     *    and immediately trims the log up to the checkpoint position.
     * 3. one repeats ITERATIONS_LOW starting a fresh runtime, and instantiating the maps.
     *    they should rebuild from the latest checkpoint (if available).
     *    this thread performs some sanity checks on the map state
     *
     *    Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     *    This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */

    @Test
    public void periodicCkpointTrimTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String streamNameB = "mystreamB";
        final String author = "periodicCkpoint";
        final int sizeAdjustment = 16; // size reduction to accomodate TRACE level debugging
        final int mapSize = PARAMETERS.NUM_ITERATIONS_MODERATE / sizeAdjustment;

        myRuntime = getDefaultRuntime().connect();

        Map<String, Long> m2A = instantiateMap(streamNameA);
        Map<String, Long> m2B = instantiateMap(streamNameB);

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            for (int i = 0; i < mapSize; i++) {
                // If TrimmedException, it happens during syncStreamUnsafe.
                // We assume that the put is always successful.
                // FIXME: I think this exception should never happen here, check!
                try {
                    m2A.put(String.valueOf(i), (long) i);
                } catch (TrimmedException te) { }
                try {
                    m2B.put(String.valueOf(i), 0L);
                } catch (TrimmedException te) { }
            }
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        scheduleConcurrently(1, ignored_task_num -> {
            CorfuRuntime currentRuntime = getMyRuntime();
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {

                // i'th checkpoint
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                long checkpointAddress = mcw1.appendCheckpoints(currentRuntime, author);

                // Trim the log
                currentRuntime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
                currentRuntime.getAddressSpaceView().gc();
                currentRuntime.getAddressSpaceView().invalidateServerCaches();
                currentRuntime.getAddressSpaceView().invalidateClientCache();

            }
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            setRuntime();
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);

            int currentMapSize = Integer.min(localm2A.size(), localm2B.size());
            for (int i = 0; i < currentMapSize; i++) {
                Object gotval; // Use intermediate var for logging in error cases

                gotval = localm2A.get(String.valueOf(i));
                log.trace("Check localm2A.get({}) -> {} by {}", i, gotval, Thread.currentThread().getName());
                if (gotval == null) {
                    log.error("Null value at key {}, localm2A = {}", i, localm2A.toString());
                }
                assertThat((Long) gotval).describedAs(Thread.currentThread().getName() + " A index " + i)
                        .isEqualTo((long) i);

                gotval = localm2B.get(String.valueOf(i));
                log.trace("Check localm2B.get({}) -> {} by {} {}", i, gotval, Thread.currentThread().getName(), Thread.currentThread().hashCode());
                if (gotval == null) {
                    log.error("Null value at key {}, localm2B = {}", i, localm2B.toString());
                }
                assertThat((Long) gotval).describedAs(Thread.currentThread().getName() + " B index " + i)
                        .isEqualTo((long) 0);
            }
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        setRuntime();
        Map<String, Long> localm2A = instantiateMap(streamNameA);
        Map<String, Long> localm2B = instantiateMap(streamNameB);
        for (int i = 0; i < mapSize; i++) {
            assertThat(localm2A.get(String.valueOf(i)) ).isEqualTo((long)i);
            assertThat(localm2A).hasSize(mapSize);
            assertThat(localm2B.get(String.valueOf(i)) ).isEqualTo(0L);
            assertThat(localm2B).hasSize(mapSize);
        }
    }

    /**
     * This test verifies that a client that recovers a map from checkpoint,
     * but wants the map at a snapshot -earlier- than the snapshot,
     * will get the right version of the map.
     *
     * It works as follows.
     * We build a map with one hundred entries [0, 1, 2, 3, ...., 99].
     *
     * Note that, each entry is one put, so if a TX starts at snapshot at 77, it should see a map with 77 items 0, 1, 2, ..., 76.
     * We are going to verify that this works even if we checkpoint the map, and trim a prefix, say of the first 50 put's.
     *
     * First, we then take a checkpoint of the map.
     *
     * Then, we prefix-trim the log up to position 50.
     *
     * Now, we start a new runtime and instantiate this map. It should build the map from a snapshot.
     *
     * Finally, we start a snapshot-TX at timestamp 77. We verify that the map state is [0, 1, 2, 3, ..., 76].
     */
    @Test
    public void undoCkpointTest() throws Exception {
        final String streamNameA = "mystreamA";
        final String author = "undoCkpoint";
        final int mapSize = 100;
        final int trimPosition = mapSize / 2;
        final int snapshotPosition = trimPosition + 20;

        myRuntime = getDefaultRuntime().connect();

        t(1, () -> {
                    Map<String, Long> m2A = instantiateMap(streamNameA);

                    // first, populate the map
                    for (int i = 0; i < mapSize; i++) {
                        m2A.put(String.valueOf(i), (long) i);
                    }

                    // ---- This test works if we remove the checkpoint and trim: ----

                    // now, take a checkpoint and perform a prefix-trim
                    MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                    mcw1.addMap((SMRMap) m2A);
                    long checkpointAddress = mcw1.appendCheckpoints(getMyRuntime(), author);

                    // Trim the log
                    getMyRuntime().getAddressSpaceView().prefixTrim(trimPosition);
                    getMyRuntime().getAddressSpaceView().gc();
                    getMyRuntime().getAddressSpaceView().invalidateServerCaches();
                    getMyRuntime().getAddressSpaceView().invalidateClientCache();

                    // ---- to here -----

        }
        );


        // start a new runtime
        t(2, () -> {
            setRuntime();

            Map<String, Long> localm2A = instantiateMap(streamNameA);

            // start a snapshot TX at position snapshotPosition
            getMyRuntime().getObjectsView().TXBuild()
                    .setType(TransactionType.SNAPSHOT)
                    .setSnapshot(snapshotPosition-1)
                    .begin();

            // finally, instantiate the map for the snapshot and assert is has the right state
            localm2A.get(0);
            assertThat(localm2A.size())
                    .isEqualTo(snapshotPosition);

            // check map positions 0..(snapshot-1)
            for (int i = 0; i < snapshotPosition; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo((long) i);
            }

            // check map positions snapshot..(mapSize-1)
            for (int i = snapshotPosition; i < mapSize; i++) {
                assertThat(localm2A.get(String.valueOf(i)))
                        .isEqualTo(null);
            }
        }
        );

    }
}
