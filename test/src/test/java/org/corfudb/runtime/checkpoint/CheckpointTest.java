package org.corfudb.runtime.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by dmalkhi on 5/25/17.
 */
@Slf4j
public class CheckpointTest extends AbstractObjectTest {

    Map<String, Long> openMap(CorfuRuntime rt, String mapName) {
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        Serializers.registerSerializer(serializer);
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        rt,
                        new TypeToken<SMRMap<String, Long>>() {
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

    private CorfuRuntime getARuntime() {
        return getNewRuntime(getDefaultNode()).connect();
    }

    /**
     * checkpoint the maps
     */
    void mapCkpoint(CorfuRuntime rt, SMRMap... maps) throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            for (SMRMap map : maps) {
                mcw1.addMap(map);
            }

            mcw1.appendCheckpoints(rt, author);
        }
    }

    volatile Token lastValidCheckpoint = Token.UNINITIALIZED;

    /**
     * Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     *
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSize, boolean expectedFullsize, boolean trimCanHappen) {
        CorfuRuntime rt = getARuntime();
        try {
            Map<String, Long> localm2A = openMap(rt, streamNameA);
            Map<String, Long> localm2B = openMap(rt, streamNameB);
            for (int i = 0; i < localm2A.size(); i++) {
                assertThat(localm2A.get(String.valueOf(i))).isEqualTo((long) i);
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

        CorfuRuntime rt = getARuntime();

        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        // thread 1: pupolates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(mapSize, mapA, mapB);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (SMRMap) mapA, (SMRMap) mapB);
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

        CorfuRuntime rt = getARuntime();
        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (SMRMap) mapA, (SMRMap) mapB);
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

        CorfuRuntime rt = getARuntime();

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

        CorfuRuntime rt = getARuntime();
        Map<String, Long> mapA = openMap(rt, streamNameA);
        Map<String, Long> mapB = openMap(rt, streamNameB);

        // pre-populate map
        populateMaps(mapSize, mapA, mapB);

        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, (SMRMap) mapA, (SMRMap) mapB);
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
    
    @Test
    public void prefixTrimTwiceAtSameAddress() throws Exception {
        final int mapSize = 5;
        CorfuRuntime rt = getARuntime();
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

        CorfuRuntime rt = getARuntime();
        CorfuRuntime runtime = getARuntime();

        try {
            Map<String, Long> testMap = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, Long>>() {
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
                    assertThat(localTestMap.get(String.valueOf(i))).isEqualTo((long) i);
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
}
