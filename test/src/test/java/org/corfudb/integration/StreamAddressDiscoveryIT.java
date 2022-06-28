package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * This class provides a set of tests to:
 *
 * 1. Validate functional behaviour of stream's address discovery mechanisms, under certain scenarios.
 *    These mechanisms are:
 *       (a) follow backpointers &
 *       (b) stream address maps (bitmaps).
 * 2. Compare performance of both, for the same described scenarios (example: reading in presence of holes,
 * reading a large stream from a fresh runtime).
 */
@Slf4j
public class StreamAddressDiscoveryIT extends AbstractIT {
    private final TypeToken<CorfuTable<Integer, String>> typeToken = new TypeToken<CorfuTable<Integer, String>>() {
    };

    private final TypeToken<CorfuTable<String, String>> typeTokenStr = new TypeToken<CorfuTable<String, String>>() {
    };

    private final String cpAuthor = "checkpointer-test";

    private CorfuRuntime createDefaultRuntimeUsingAddressMaps() {
        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT)
                .setCacheDisabled(false);
        runtime.getParameters().setStreamBatchSize(PARAMETERS.NUM_ITERATIONS_LOW);
        return runtime;
    }

    private long readFromNewRuntimeUsingAddressMaps(String streamName, int expectedSize) {
        return readFromNewRuntime(createDefaultRuntimeUsingAddressMaps(), streamName, expectedSize);
    }

    private long readFromNewRuntime(CorfuRuntime rt, String streamName, int expectedSize) {
        try {
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(typeToken)
                    .setStreamName(streamName)
                    .open();

            long startTime = System.currentTimeMillis();
            assertThat(table.size()).isEqualTo(expectedSize);
            return System.currentTimeMillis() - startTime;
        } finally {
            rt.shutdown();
        }
    }

     /**
     * This test aims to validate a stream rebuilt when holes are present. At the same time it is a very
     * small scale test for benchmarking stream rebuilt in the presence of holes when using backpointers
     * vs. using stream maps.
     *
     * Steps to reproduce this test:
     * - Write 10000 entries to S1.
     * - Write 10000 entries to S2.
     * - Insert a hole for S1.
     * - Write 100 entries to S1.
     *
     * - From a new (fresh) runtime access S1:
     *      - First, using followBackpointers as the address discovery mechanism.
     *      - Second, using streamMaps as the address discovery mechanism.
     *
     * Compare times for both mechanisms, ensure stream maps is faster than following backpointers
     * (which will single step through 10.000 entries)
     *
     * @throws Exception error
     */
    @Test
    public void benchMarkStreamRebuiltInPresenceOfHoles() throws Exception {
        final String stream1Name = "stream1";
        final String stream2Name = "stream2";

        // Create Server & Runtime
        Process server = runDefaultServer();
        // Runtime writers
        runtime = createRuntimeWithCache();

        try {
            // Write 10K entries on S1 & S2
            CorfuTable<Integer, String> table1 = runtime.getObjectsView().build()
                    .setTypeToken(typeToken)
                    .setStreamName(stream1Name)
                    .open();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
                table1.put(i, String.valueOf(i));
            }

            // Write 10K entries on S2
            CorfuTable<Integer, String> table2 = runtime.getObjectsView().build()
                    .setTypeToken(typeToken)
                    .setStreamName(stream2Name)
                    .open();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
                table2.put(i, String.valueOf(i));
            }

            // Force a hole for S1
            Token token = runtime.getSequencerView().next(CorfuRuntime.getStreamID(stream1Name)).getToken();
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient("tcp://localhost:9000")
                    .write(LogData.getHole(token));

            // Write 100 more entries for S1
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                table1.put(i, String.valueOf(i));
            }

            // Read S1 from new runtime (retrieving address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps(stream1Name,
                    PARAMETERS.NUM_ITERATIONS_LARGE);
            log.debug("**** Total time new runtime to sync 'Stream 1' (address maps): "
                    + totalTimeAddressMaps);
        } finally {
            shutdownCorfuServer(server);
        }
    }

    /**
     *  This test checkpoints two streams separately and trims on the lower checkpoint boundary.
     *  The objective is to test that a stream is rebuilt from a checkpoint with updates to the
     *  regular stream still present in the log (addresses 10 and 11)
     *
     *         S1  S2  S1  S2  S2  S1 cp1 cp1 cp1 s1   s2   s2   cp2  cp2  cp2   s2
     *       +---------------------------------------------------------------------+
     *       | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
     *       +---------------------------------------------------------------------+
     *
     *  S1: Stream 1
     *  S2: Stream 2
     *  CP1: Checkpoint S1
     *  CP2: Checkpoint S2
     *
     * @throws Exception error
     */
    @Test
    public void checkpointAndTrimAtDifferentPoint() throws Exception {

        Process server = runDefaultServer();
        CorfuRuntime defaultRT = createDefaultRuntime();
        CorfuRuntime rt = createDefaultRuntime();

        try {
            final int sizeMap1 = 4;
            final int sizeMap2 = 6;

            StreamingMap<String, String> map1 = defaultRT.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream1")
                    .open();

            StreamingMap<String, String> map2 = defaultRT.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream2")
                    .open();

            // Writes as described in the comments
            transactionalWrite(defaultRT, map1, "0", "0");
            transactionalWrite(defaultRT, map2, "1", "1");
            transactionalWrite(defaultRT, map1, "2", "2");
            transactionalWrite(defaultRT, map2, "3", "3");
            transactionalWrite(defaultRT, map2, "4", "4");
            transactionalWrite(defaultRT, map1, "5", "5");

            // Checkpoint S1
            MultiCheckpointWriter<StreamingMap<String, String>> mcw1 = new MultiCheckpointWriter<>();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map1, "9", "9");
            transactionalWrite(defaultRT, map2, "10", "10");
            transactionalWrite(defaultRT, map2, "11", "11");

            // Checkpoint S2
            MultiCheckpointWriter<StreamingMap<String, String>> mcw2 = new MultiCheckpointWriter<>();
            mcw2.addMap(map2);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map2, "15", "15");

            assertThat(map1).hasSize(sizeMap1);
            assertThat(map2).hasSize(sizeMap2);

            // Trim on the lower address (@5)
            defaultRT.getAddressSpaceView().prefixTrim(minCheckpointAddress);

            // New runtime read s1, read s2 (from checkpoint)
            Map<String, String> map1rt = rt.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream1")
                    .open();

            Map<String, String> map2rt = rt.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream2")
                    .open();

            assertThat(map1rt.size()).isEqualTo(sizeMap1);
            assertThat(map2rt.size()).isEqualTo(sizeMap2);
        } finally {
            defaultRT.shutdown();
            rt.shutdown();
            shutdownCorfuServer(server);
        }
    }

    private void transactionalWrite(CorfuRuntime rt, Map<String, String> map, String key, String value) {
        rt.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
        map.put(key, value);
        rt.getObjectsView().TXEnd();
    }

    /**
     *
     *    This test validates that a snapshot transaction can be completed
     *    between two checkpoints, whenever part of the address space below the
     *    first checkpoint has been trimmed.
     *
     *
     *         S1  S1  S1  S1  S1  No [   CP1   ]  S1  S1  S1   S1   S1  No-Op [     CP2      ]
     *                             Op
     *       +-------------------------------------------------------------------------------+
     *       | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 |
     *       +-------------------------------------------------------------------------------+
     *                     ^                       ^
     *                    TRIM                  SNAPSHOT
     *
     *  S1: Stream 1
     *  CP1: Checkpoint 1 to S1
     *  CP2: Checkpoint 2 to S1
     *
     * @throws Exception error
     */
    @Test
    public void checkpointAndTrimAtDifferentPointSnapshot() throws Exception {

        Process server = runDefaultServer();
        CorfuRuntime writeRuntime = createDefaultRuntime();
        CorfuRuntime readRuntime = createDefaultRuntime();

        try {
            final int batchWrite = 5;
            final long trimAddress = 3L;
            final long snapshotAddress = 9L;
            final int sizeAtSnapshot = 6;

            StreamingMap<String, String> map1 = writeRuntime.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream1")
                    .open();

            for(int i=0; i<batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 1
            MultiCheckpointWriter<StreamingMap<String, String>> mcw1 = new MultiCheckpointWriter<>();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(writeRuntime, "author");

            for(int i=batchWrite; i<batchWrite + batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 2
            MultiCheckpointWriter<StreamingMap<String, String>> mcw2 = new MultiCheckpointWriter<>();
            mcw2.addMap(map1);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(writeRuntime, "author");

            assertThat(map1).hasSize(batchWrite+batchWrite);

            // Trim below lower checkpoint
            writeRuntime.getAddressSpaceView().prefixTrim(new Token(minCheckpointAddress.getEpoch(), trimAddress));

            // New runtime
            Map<String, String> map1rt = readRuntime.getObjectsView().build()
                    .setTypeToken(typeTokenStr)
                    .setStreamName("stream1")
                    .open();

            // Start snapshot transaction between both snapshots
            readRuntime.getObjectsView().TXBuild().type(TransactionType.SNAPSHOT)
                    .snapshot(new Token(maxCheckpointAddress.getEpoch(), snapshotAddress))
                    .build()
                    .begin();
            assertThat(map1rt).hasSize(sizeAtSnapshot);
            readRuntime.getObjectsView()
                    .TXEnd();
        } finally {
            writeRuntime.shutdown();
            readRuntime.shutdown();
            shutdownCorfuServer(server);
        }
    }

    /**
     *
     * In this test we want to verify stream's address space rebuilt from log unit given that a valid checkpoint
     * appears after entries to the regular stream. We aim to validate trim mark is properly set despite ordering.
     *
     * Test Case 0:
     *
     *         S1  S1  S1      S1  S2  S2        S1  S1  S1  S1    S1    S1   S1   S1  CP-S1 snapshot @7 (tail)
     *       +---------------------------    +-----------------------------------------------+-------+
     *       | 0 | 1 | 2 | ..| 7 | 8 | 9 |    | 10 | 6 | 7 | 8 | ..... | 11 | 19 | 20 | 21 | 22 | 23 | ...
     *       +---------------------------    +-----------------------------------------------+-------+
     *                          ^
     *                         TRIM
     **/
    @Test
    public void testStreamRebuilt() throws Exception {

        List<CorfuRuntime> runtimes = new ArrayList<>();

        final int insertions = 10;
        final int insertionsB = 2;
        final String stream1 = "mapA";
        final String stream2 = "mapB";
        final int snapshotAddress = 7;

        // Run Corfu Server
        Process server = runDefaultServer();

        try {
            runtime = createDefaultRuntime();
            runtimes.add(runtime);

            // Open mapA (S1) and mapB (S2)
            StreamingMap<String, Integer> mapA = createMap(runtime, stream1);
            StreamingMap<String, Integer> mapB = createMap(runtime, stream2);

            // Write 8 entries to mapA
            for (int i = 0; i < insertions - insertionsB; i++) {
                mapA.put(String.valueOf(i), i);
            }

            // Write 2 entries to mapB
            for (int i = 0; i < insertionsB; i++) {
                mapB.put(String.valueOf(i), i);
            }

            // Write 10 more entries to streamA (emulating writes that came in between the time a snapshot was taken
            // for a checkpoint and actual checkpoint entries were written)
            for (int i = insertions; i < insertions * 2; i++) {
                mapA.put(String.valueOf(i), i);
            }

            // Start checkpoint with snapshot time 9 for mapA
            CheckpointWriter<StreamingMap<String, Integer>> cpw = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(stream1),
                    cpAuthor, mapA
            );
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress), Optional.empty());

            // Start checkpoint with snapshot time 9 for mapB
            CheckpointWriter<StreamingMap<String, Integer>> cpwB = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(stream2),
                    cpAuthor, mapB
            );
            cpwB.appendCheckpoint(new Token(0, snapshotAddress), Optional.empty());

            // Trim the log
            runtime.getAddressSpaceView().prefixTrim(cpAddress);
            runtime.getAddressSpaceView().gc();
            runtime.getAddressSpaceView().invalidateServerCaches();
            runtime.getAddressSpaceView().invalidateClientCache();

            // Restart the server
            assertThat(shutdownCorfuServer(server)).isTrue();
            server = runDefaultServer();

            // Start new runtime
            CorfuRuntime runtimeRestart = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
            runtimes.add(runtimeRestart);

            // Fetch Address Space for the given stream S1
            StreamAddressSpace addressSpaceA = Utils.getLogAddressSpace(runtimeRestart
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(stream1));

            // Verify address space and trim mark is properly set for the given stream (should be 7 which  is the start log address
            // for the existing checkpoint)
            assertThat(addressSpaceA.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceA.size()).isEqualTo(insertions);

            // Fetch Address Space for the given stream S2
            StreamAddressSpace addressSpaceB =  Utils.getLogAddressSpace(runtimeRestart
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(stream2));

            // Verify address space and trim mark is properly set for the given stream (should be 7 which  is the start log address
            // for the existing checkpoint)
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceB.size()).isEqualTo(insertionsB);

            // Open mapB after restart (verify it loads from checkpoint)
            Map<String, Integer> mapBRestart = createMap(runtimeRestart, stream2);
            assertThat(mapBRestart).hasSize(insertionsB);

            // Open mapA after restart (verify it loads from checkpoint)
            Map<String, Integer> mapARestart = createMap(runtimeRestart, stream1);
            assertThat(mapARestart).hasSize(insertions*2 - insertionsB);

        } finally {
            runtimes.forEach(CorfuRuntime::shutdown);
            shutdownCorfuServer(server);
        }
    }


    /**
     *
     *  In this test we want to verify stream's address space rebuilt from log unit given that a hole is the first
     *  valid address for a stream after a trim (i.e., backpointer is lost) and a checkpoint is present.
     *
     * Test Case 1:
     *
     *         S1  S1  S1   S1   S1  S2        S1       S1  S1  S1    S1    S1   S1   S1     CP-S1
     *       +-------------------------    +-----------------------------------------------+--------------+
     *       | 0 | 1 | 2 | ... | 8 | 9 |    | 10 (hole) | 6 | 7 | 8 | ..... | 11 | 19 | 20 | 21 | 22 | 23 |
     *       +-------------------------    +-----------------------------------------------+--------------+
     *                             ^
     *                           TRIM
     *
     * @throws Exception error
     */
    @Test
    public void testStreamRebuiltWithHoleAsFirstEntryAfterTrim() throws Exception {

        List<CorfuRuntime> runtimes = new ArrayList<>();

        final int insertions = 10;
        final String streamNameA = "mapA";
        final String streamNameB = "mapB";
        final int snapshotAddress = 8;
        final long checkpointStartRecord = 21L;

        // Run Corfu Server
        Process server = runDefaultServer();

        try {
            runtime = createDefaultRuntime();
            runtimes.add(runtime);

            // Open mapA
            StreamingMap<String, Integer> mapA = createMap(runtime, streamNameA);
            StreamingMap<String, Integer> mapB = createMap(runtime, streamNameB);

            // Write 9 entries to mapA
            for (int i = 0; i < insertions - 1; i++) {
                mapA.put(String.valueOf(i), i);
            }

            mapB.put("a", 0);

            // Force a hole for streamA
            Token token = runtime.getSequencerView().next(CorfuRuntime.getStreamID(streamNameA)).getToken();
            LogData hole = LogData.getHole(token);
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient(NodeLocator.builder()
                            .host(DEFAULT_HOST)
                            .port(DEFAULT_PORT)
                            .build()
                            .toEndpointUrl())
                    .write(hole);

            // Write 10 more entries to streamA (emulating writes that came in between the time a snapshot was taken
            // for a checkpoint and actual checkpoint entries were written)
            for (int i = insertions; i < insertions * 2; i++) {
                mapA.put(String.valueOf(i), i);
            }

            // Start checkpoint with snapshot time (right before the hole) - Ignore the fact the entry from streamB is lost
            // we're interested in verifying the behaviour of streamA with end address != trim address.
            CheckpointWriter<StreamingMap<String, Integer>> cpw = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamNameA),
                    "checkpoint-test", mapA);
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress), Optional.empty());

            // Trim the log
            runtime.getAddressSpaceView().prefixTrim(cpAddress);
            runtime.getAddressSpaceView().gc();
            runtime.getAddressSpaceView().invalidateServerCaches();
            runtime.getAddressSpaceView().invalidateClientCache();

            // Restart the server
            assertThat(shutdownCorfuServer(server)).isTrue();
            server = runDefaultServer();

            // Start NEW runtime
            CorfuRuntime runtimeRestart = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
            runtimes.add(runtimeRestart);

            // Verify checkpoint START_LOG_ADDRESS
            LogEntry cpStart = (CheckpointEntry) runtimeRestart.getAddressSpaceView().read(checkpointStartRecord)
                    .getPayload(runtimeRestart);
            assertThat(((CheckpointEntry) cpStart).getDict()
                    .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)).isEqualTo("8");

            // Fetch Address Space for the given stream
            StreamAddressSpace addressSpaceA = Utils.getLogAddressSpace(runtimeRestart
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameA));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceA.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceA.size()).isEqualTo(insertions);

            // Open mapA after restart (verify it loads from checkpoint)
            Map<String, Integer> mapARestart = createMap(runtimeRestart, streamNameA);
            assertThat(mapARestart).hasSize(insertions * 2 - 1);
        } finally {
            runtimes.forEach(CorfuRuntime::shutdown);
            shutdownCorfuServer(server);
        }
    }


    /**
     *
     *   In this test we want to verify stream's address space rebuilt from log unit given that a hole is the first
     *   valid address for a stream after a trim (i.e., backpointer is lost) and no checkpoint is present, i.e.,
     *   S2 was never written to before the checkpoint.
     *
     * Test Case 2:
     *
     *         S1  S1  S1   S1   S1  S1        S2       S2   S2  S2    S2    S2   S2
     *       +-------------------------     +---------------------------------------+
     *       | 0 | 1 | 2 | ... | 8 | 9 |    | 10 (hole) | 11 | 12 | 13 | ..... | 19 |
     *       +-------------------------     +---------------------------------------+
     *                                ^
     *                              TRIM
     *
     * @throws Exception error
     */
    @Test
    public void testStreamRebuiltWithHoleAsFirstEntryAfterTrimNoCP() throws Exception {

        List<CorfuRuntime> runtimes = new ArrayList<>();

        final int insertions = 10;
        final String streamNameA = "mapA";
        final String streamNameB = "mapB";
        final int snapshotAddress = 10;

        // Run Corfu Server
        Process server = runDefaultServer();

        try {
            runtime = createDefaultRuntime();
            runtimes.add(runtime);

            // Open mapA
            StreamingMap<String, Integer> mapA = createMap(runtime, streamNameA);
            StreamingMap<String, Integer> mapB = createMap(runtime, streamNameB);

            // Write 9 entries to mapA
            for (int i = 0; i < insertions; i++) {
                mapA.put(String.valueOf(i), i);
            }

            // Force a hole for streamB
            Token token = runtime.getSequencerView().next(CorfuRuntime.getStreamID(streamNameB)).getToken();
            LogData hole = LogData.getHole(token);
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient("tcp://localhost:9000")
                    .write(hole);

            // Write 10 entries to stream B
            for (int i = 0; i < insertions; i++) {
                mapB.put(String.valueOf(i), i);
            }

            // Checkpoint A with snapshot @ 9
            CheckpointWriter<StreamingMap<String, Integer>> cpw = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamNameA), cpAuthor, mapA);
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress - 1), Optional.empty());

            // Trim the log
            runtime.getAddressSpaceView().prefixTrim(cpAddress);
            runtime.getAddressSpaceView().gc();
            runtime.getAddressSpaceView().invalidateServerCaches();
            runtime.getAddressSpaceView().invalidateClientCache();

            // Before restarting the server instantiate new runtime and open map to validate it
            // is correctly built from checkpoint.
            CorfuRuntime rt2 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
            runtimes.add(rt2);

            // Fetch Address Space for the given stream
            StreamAddressSpace addressSpaceB = Utils.getLogAddressSpace(rt2
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameB));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(Address.NON_EXIST);
            assertThat(addressSpaceB.size()).isEqualTo(insertions);

            // Open mapB new runtime
            Map<String, Integer> mapBNewRuntime = createMap(rt2, streamNameB);
            assertThat(mapBNewRuntime).hasSize(insertions);

            // Open mapA new runtime
            Map<String, Integer> mapANewRuntime = createMap(rt2, streamNameA);
            assertThat(mapANewRuntime).hasSize(insertions);

            // Restart the server
            assertThat(shutdownCorfuServer(server)).isTrue();
            server = runDefaultServer();

            // Start NEW runtime
            CorfuRuntime runtimeRestart = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
            runtimes.add(runtimeRestart);

            // Fetch Address Space for the given stream
            addressSpaceB = Utils.getLogAddressSpace(runtimeRestart
                    .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameB));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(Address.NON_EXIST);
            assertThat(addressSpaceB.size()).isEqualTo(insertions);

            // Open mapB after restart
            Map<String, Integer> mapBRestart = createMap(runtimeRestart, streamNameB);
            assertThat(mapBRestart).hasSize(insertions);

            // Open mapA after restart
            Map<String, Integer> mapARestart = createMap(runtimeRestart, streamNameA);
            assertThat(mapARestart).hasSize(insertions);
        } finally {
            runtimes.forEach(CorfuRuntime::shutdown);
            shutdownCorfuServer(server);
        }
    }

    /**
     * Test rebuilding a stream from a new runtime, whenever the last address before the checkpoint
     * was a hole.
     *
     * This case is interesting to test as addresses that become holes are discarded by the streamView,
     * while sequencer's are agnostic of this info, hence, take it into account for trim mark computation.
     */
    @Test
    public void testCheckpointWhenLastAddressIsHole() throws Exception {
        final int numEntries = 10;

        // Run Corfu Server
        Process server = runDefaultServer();

        try {
            // Create Runtime
            runtime = createDefaultRuntime();

            // Instantiate streamA and streamB as maps
            final String streamA = "streamA";
            final String streamB = "streamB";
            StreamingMap<String, Integer> mA = createMap(runtime, streamA);
            StreamingMap<String, Integer> mB = createMap(runtime, streamB);

            // Write 10 Entries to streamA
            for (int i = 0; i < numEntries; i++) {
                mA.put(String.valueOf(i), i);
            }

            // Force a hole as the last update to streamA
            Token token = runtime.getSequencerView().next(CorfuRuntime.getStreamID(streamA)).getToken();
            LogData hole = LogData.getHole(token);
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient(NodeLocator.builder()
                            .host(DEFAULT_HOST)
                            .port(DEFAULT_PORT)
                            .build()
                            .toEndpointUrl())
                    .write(hole);

            // Write 10 Entries to streamB
            for (int i = 0; i < numEntries; i++) {
                mB.put(String.valueOf(i), i);
            }

            // Start a CheckpointWriter for streamA
            CheckpointWriter<StreamingMap<String, Integer>> cpwA = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamA), cpAuthor, mA);
            Token cpTokenA = cpwA.appendCheckpoint();

            // Start a CheckpointWriter for streamB
            CheckpointWriter<StreamingMap<String, Integer>> cpwB = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamB), cpAuthor, mB);
            cpwB.appendCheckpoint();

            // Add an update to streamA after checkpoint
            mA.put(String.valueOf(numEntries), numEntries);

            // Trim the log at B's CPToken
            runtime.getAddressSpaceView().prefixTrim(cpTokenA);

            // Flush Server Cache after trim
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient(NodeLocator.builder()
                            .host(DEFAULT_HOST)
                            .port(DEFAULT_PORT)
                            .build()
                            .toEndpointUrl())
                    .flushCache();

            // Instantiate streamA from new Runtime, so stream is rebuilt
            CorfuRuntime rt2 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
            Map<String, Integer> mA2 = createMap(rt2, streamA);

            // By accessing the map, we ensure we are able to load from the checkpoint.
            try {
                rt2.getObjectsView().TXBuild()
                        .type(TransactionType.OPTIMISTIC)
                        .build()
                        .begin();
                assertThat(mA2.size()).isEqualTo(numEntries + 1);

                // Check Data
                for (int i = 0; i <= numEntries; i++) {
                    assertThat(mA2.get(String.valueOf(i))).isEqualTo(i);
                }
            } finally {
                rt2.getObjectsView().TXEnd();
                rt2.shutdown();
                runtime.shutdown();
            }
        } finally {
            shutdownCorfuServer(server);
        }
    }

    /**
     * This test creates an empty checkpoint (for an empty stream) and resets the server to verify
     * that sequencer bootstrap is correct and stream is correctly built from checkpoint.
     *
     * @throws Exception error
     */
    @Test
    public void testEmptyCheckpointRebuiltOnRestart() throws Exception {
        // Run Corfu Server
        Process server = runDefaultServer();

        // Create Runtime
        runtime = createDefaultRuntime();

        // Runtime After Restart
        CorfuRuntime rtRestart = null;

        try {
            // Instantiate streamA as map
            final String streamA = "streamA";
            StreamingMap<String, Integer> map = createMap(runtime, streamA);

            // Start a CheckpointWriter for streamA (empty)
            CheckpointWriter<StreamingMap<String, Integer>> cpwA = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamA),
                    cpAuthor, map);
            Token cpToken = cpwA.appendCheckpoint();

            // Verify Checkpoint Token
            assertThat(cpToken.getSequence()).isEqualTo(0L);

            // Verify Checkpoint START_LOG_ADDRESS (reading start record)
            // Because the stream was empty, it should force a hole on 0, and this should be the start address
            LogEntry cpStart = (CheckpointEntry) runtime.getAddressSpaceView().read(1L).getPayload(runtime);
            assertThat(((CheckpointEntry) cpStart).getDict()
                    .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)).isEqualTo("0");

            // Trim the log at B's CPToken
            runtime.getAddressSpaceView().prefixTrim(cpToken);

            // Flush Server Cache after trim
            runtime.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient(NodeLocator.builder()
                            .host(DEFAULT_HOST)
                            .port(DEFAULT_PORT)
                            .build()
                            .toEndpointUrl())
                    .flushCache();

            // Restart Server
            assertThat(shutdownCorfuServer(server)).isTrue();
            server = runDefaultServer();

            // Start a new runtime
            rtRestart = new CorfuRuntime(DEFAULT_ENDPOINT).connect();

            // Instantiate streamA as map after restart (verify it can load from empty checkpoint)
            Map<String, Integer> mapRestart = createMap(rtRestart, streamA);
            assertThat(mapRestart).hasSize(0);

        } finally {
            shutdownCorfuServer(server);

            if (runtime != null) runtime.shutdown();
            if (rtRestart != null) rtRestart.shutdown();
        }
    }

    String getConnectionString(int port) {
        return AbstractIT.DEFAULT_HOST + ":" + port;
    }

    /**
     * This test aims to verify sequencer reconfiguration from log units in the event of empty maps.
     * Steps to reproduce this test:
     * 1. Open a stream.
     * 2. Checkpoint the stream (this will force a hole on the empty map from the checkpointer).
     * 3. Verify Stream Maps from head Log unit and sequencer tails.
     * 4. PrefixTrim
     * 5. Enforce sequencer failover (shutdown primary sequencer server).
     * 6. Verify Stream Maps from log unit and sequencer tails (both regular stream and checkpoint
     * stream should be present).
     *
     * @throws Exception error
     */
    @Test
    public void testCheckpointEmptyMapAndSequencerFailover() throws Exception {
        testCheckpointEmptyMapWithSequencerFailover(false);
    }

    /**
     * This test is similar to the previous, but enforces the hole from a reader, as reader hole fill
     * does not contain stream information to build address map.
     * @throws Exception error
     */
    @Test
    public void testCheckpointEmptyMapWithReaderHoleAndSequencerFailover() throws Exception {
        testCheckpointEmptyMapWithSequencerFailover(true);
    }

    private void testCheckpointEmptyMapWithSequencerFailover(boolean readerHole) throws Exception{
        final int n0Port = 9000;
        final int n1Port = 9001;
        final int n2Port = 9002;

        final int clusterSizeN2 = 2;
        final int clusterSizeN3 = 3;

        final int workflowNumRetry = 3;
        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofMillis(50);

        // Run 3 Corfu Server
        Process server_1 = runServer(n0Port, true);
        Process server_2 = runServer(n1Port, false);
        Process server_3 = runServer(n2Port, false);

        runtime = new CorfuRuntime(DEFAULT_ENDPOINT).connect();

        runtime.getManagementView().addNode(getConnectionString(n1Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        runtime.getManagementView().addNode(getConnectionString(n2Port), workflowNumRetry,
                timeout, pollPeriod);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);

        final long totalEntries = 2L;
        int extraEntry = 0;

        try {
            // Instantiate streamA as map
            final String streamA = "streamA";
            StreamingMap<String, Integer> mA = createMap(runtime, streamA);

            UUID streamID = CorfuRuntime.getStreamID(streamA);
            UUID checkpointId = CorfuRuntime.getCheckpointStreamIdFromId(streamID);

            if (readerHole) {
                // Generate hole from a reader
                runtime.getSequencerView().next(streamID).getToken();
                assertThat(mA.size()).isEqualTo(0);
                extraEntry = 1;
            }

            // Start a CheckpointWriter for streamA
            CheckpointWriter<StreamingMap<String, Integer>> cpwA = new CheckpointWriter<>(
                    runtime, CorfuRuntime.getStreamID(streamA),
                    cpAuthor, mA);
            Token cpTokenA = cpwA.appendCheckpoint();

            // Verify Address Maps from Log Unit and Sequencer Tails (first node)
            StreamsAddressResponse logUnitResponse = Utils.getLogAddressSpace(runtime
                    .getLayoutView().getRuntimeLayout());

            TokenResponse sequencerTails = runtime.getSequencerView().query(streamID, checkpointId);

            // +1 because checkpointer contributes to a NO_OP entry
            assertThat(logUnitResponse.getAddressMap().get(checkpointId).getTail()).isEqualTo(totalEntries + extraEntry);
            assertThat(logUnitResponse.getAddressMap().get(streamID).getTail()).isEqualTo(extraEntry);

            assertThat(sequencerTails.getStreamTail(streamID)).isEqualTo(extraEntry);
            assertThat(sequencerTails.getStreamTail(checkpointId)).isEqualTo(totalEntries + extraEntry);

            // Trim the log at A's CPToken
            runtime.getAddressSpaceView().prefixTrim(cpTokenA);

            long currentEpoch = runtime.getLayoutView().getLayout().getEpoch();

            // Shutdown A
            shutdownCorfuServer(server_1);

            waitForLayoutChange(layout -> layout.getEpoch() > currentEpoch, runtime);

            // Verify Address Maps from Log Unit and Sequencer Tails (second node)
            logUnitResponse = Utils.getLogAddressSpace(runtime.getLayoutView().getRuntimeLayout());
            sequencerTails = runtime.getSequencerView().query(streamID, checkpointId);

            assertThat(logUnitResponse.getAddressMap().get(checkpointId).getTail()).isEqualTo(totalEntries + extraEntry);
            assertThat(logUnitResponse.getAddressMap().get(streamID).getTail()).isEqualTo(extraEntry);

            assertThat(sequencerTails.getStreamTail(streamID)).isEqualTo(extraEntry);
            assertThat(sequencerTails.getStreamTail(checkpointId)).isEqualTo(totalEntries + extraEntry);
        } finally {
            shutdownCorfuServer(server_2);
            shutdownCorfuServer(server_3);
        }
    }
}
