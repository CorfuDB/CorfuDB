package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
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
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


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
public class StreamAddressDiscoveryIT extends AbstractIT {

    private CorfuRuntime createDefaultRuntimeUsingFollowBackpointers() {
        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT);
        runtime.getParameters().setFollowBackpointersEnabled(true)
                .setCacheDisabled(false);
        return runtime;
    }

    private CorfuRuntime createDefaultRuntimeUsingAddressMaps() {
        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT);
        runtime.getParameters().setFollowBackpointersEnabled(false)
        .setCacheDisabled(false);
        runtime.getParameters().setStreamBatchSize(PARAMETERS.NUM_ITERATIONS_LOW);
        return runtime;
    }

    private long readFromNewRuntimeFollowingBackpointers(String streamName, int expectedSize) {
        return readFromNewRuntime(createDefaultRuntimeUsingFollowBackpointers(), streamName, expectedSize);
    }

    private long readFromNewRuntimeUsingAddressMaps(String streamName, int expectedSize) {
        return readFromNewRuntime(createDefaultRuntimeUsingAddressMaps(), streamName, expectedSize);
    }

    private long readFromNewRuntime(CorfuRuntime rt, String streamName, int expectedSize) {
        try {
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
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
     * @throws Exception
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
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName(stream1Name)
                    .open();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
                table1.put(i, String.valueOf(i));
            }

            // Write 10K entries on S2
            CorfuTable<Integer, String> table2 = runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
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

            // Read S1 from new runtime (following backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers(stream1Name,
                    PARAMETERS.NUM_ITERATIONS_LARGE);
            System.out.println("**** Total time new runtime to send 'Stream 1' (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read S1 from new runtime (retrieving address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps(stream1Name,
                    PARAMETERS.NUM_ITERATIONS_LARGE);
            System.out.println("**** Total time new runtime to send 'Stream 1' (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            shutdownCorfuServer(server);
        }
    }

    /**
     * Compare behaviour of multi-threaded writes over a shared runtime
     * and stream rebuilt.
     *
     * - Write 10K entries to S1 (runtime follow backpointers) - rt1w
     * - Write 10K entries to S1 (runtime address maps) - rt2w
     * - Compare behaviour (it is expected to be in the same order)
     * - Read 10K entries new runtime (follow backpointers) - rt1r
     * - Read 10K entries new runtime (address maps) - rt2r
     * - Compare behaviour (it is expected to be better for address maps)
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsSharedRuntime() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        // Writer runtime (follow backpointers)
        CorfuRuntime rt1w = createDefaultRuntimeUsingFollowBackpointers();

        // Writer runtime (retrieve stream address map)
        CorfuRuntime rt2w = createDefaultRuntimeUsingAddressMaps();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numKeys = PARAMETERS.NUM_ITERATIONS_LARGE;

        try {
            System.out.println("**** Start multi-threaded benchmark");

            CorfuTable<Integer, String> table = rt1w.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Long startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor.submit(() -> {
                    rt1w.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt1w.getObjectsView().TXEnd();
                });
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for follow backpointers)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            CorfuTable<Integer, String> table2 = rt2w.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor2 = Executors.newFixedThreadPool(numThreads);
            startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor2.submit(() -> {
                    rt2w.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table2.put(value, String.valueOf(value));
                    rt2w.getObjectsView().TXEnd();
                });
            }

            executor2.shutdown();
            executor2.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for stream address maps)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (follow backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to send 'Stream 1' (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read from fresh runtime (stream address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to send 'Stream 1' (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            rt1w.shutdown();
            rt2w.shutdown();
            shutdownCorfuServer(server);
        }
    }

    /**
     * Compare behaviour of multi-threaded writes over multiple runtime's
     * and stream rebuilt.
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsMultiClients() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        Map<CorfuRuntime, CorfuTable<Integer, String>> runtimeToTable = new HashMap<>();
        List<CorfuRuntime> runtimesFollowBackpointers = new ArrayList<>();
        List<CorfuRuntime> runtimesAddressMaps = new ArrayList<>();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numClients = 10;
        final int numKeys = PARAMETERS.NUM_ITERATIONS_LARGE;

        for (int i = 0; i < numClients; i++) {
            CorfuRuntime rt = createDefaultRuntimeUsingFollowBackpointers().setCacheDisabled(false);
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();
            runtimeToTable.put(rt, table);
            runtimesFollowBackpointers.add(rt);
        }

        for (int i = 0; i < numClients; i++) {
            CorfuRuntime rt = createDefaultRuntimeUsingAddressMaps();
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();
            runtimeToTable.put(rt, table);
            runtimesAddressMaps.add(rt);
        }

        try {
            System.out.println("**** Start multi-threaded benchmark");

            // RUNTIME FOLLOW BACKPOINTERS (WRITE)
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Long startTime = System.currentTimeMillis();

            int runtimeIndex = 0;
            for (int i = 0; i < numKeys; i++) {
                if (runtimeIndex >= numClients) {
                    runtimeIndex = 0;
                }
                final CorfuRuntime rt = runtimesFollowBackpointers.get(runtimeIndex);
                CorfuTable<Integer, String> table = runtimeToTable.get(rt);
                final int value = i;
                executor.submit(() -> {
                    rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt.getObjectsView().TXEnd();
                });
                runtimeIndex++;
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for follow backpointers)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // RUNTIME ADDRESS MAPS (WRITE)
            executor = Executors.newFixedThreadPool(numThreads);
            startTime = System.currentTimeMillis();

            runtimeIndex = 0;
            for (int i = 0; i < numKeys; i++) {
                if (runtimeIndex >= numClients) {
                    runtimeIndex = 0;
                }
                final CorfuRuntime rt = runtimesAddressMaps.get(runtimeIndex);
                CorfuTable<Integer, String> table = runtimeToTable.get(rt);
                final int value = i;
                executor.submit(() -> {
                    rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt.getObjectsView().TXEnd();
                });
                runtimeIndex++;
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for stream address maps)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (follow backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to send stream (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read from fresh runtime (stream address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to send stream (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            for(CorfuRuntime rt : runtimesFollowBackpointers) {
                rt.shutdown();
            }

            for(CorfuRuntime rt : runtimesAddressMaps) {
                rt.shutdown();
            }

            shutdownCorfuServer(server);
        }
    }

    /**
     * Evaluate stream rebuilt for transactional reads.
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsReadsTx() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        // Writer Runtime
        CorfuRuntime rt1 = createRuntimeWithCache();

        // Reader Runtime (following backpointers)
        CorfuRuntime rt2 = createDefaultRuntimeUsingFollowBackpointers();

        // Reader Runtime (stream address maps)
        CorfuRuntime rt3 = createDefaultRuntimeUsingAddressMaps();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numKeys = 10000;

        try {
            System.out.println("Start multi-threaded benchmark");

            CorfuTable<Integer, String> table = rt1.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            ExecutorService executor2 = Executors.newFixedThreadPool(numThreads);
            ExecutorService executor3 = Executors.newFixedThreadPool(numThreads);

            Long startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor.submit(() -> {
                    rt1.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    table.put(value, String.valueOf(value));
                    rt1.getObjectsView().TXEnd();
                });
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in: %s ms",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (following backpointers)
            CorfuTable<Integer, String> table2 = rt2.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor2.submit(() -> {
                    rt2.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    assertThat(table2.get(value)).isEqualTo(String.valueOf(value));
                    rt2.getObjectsView().TXEnd();
                });
            }

            executor2.shutdown();
            executor2.awaitTermination(1, TimeUnit.MINUTES);

            long followBackpointersTime = System.currentTimeMillis() - startTime;

            System.out.println(String.format("**** New runtime read (following backpointers) completed in: %s ms",
                    followBackpointersTime));

            assertThat(table2.size()).isEqualTo(numKeys);

            // Read from fresh runtime (stream address map)
            CorfuTable<Integer, String> table3 = rt3.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor3.submit(() -> {
                    rt3.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    assertThat(table3.get(value)).isEqualTo(String.valueOf(value));
                    rt3.getObjectsView().TXEnd();
                });
            }

            executor3.shutdown();
            executor3.awaitTermination(1, TimeUnit.MINUTES);

            long addressMapTime = System.currentTimeMillis() - startTime;

            System.out.println(String.format("**** New runtime read (stream address maps) completed in: %s ms",
                    addressMapTime));

            assertThat(table3.size()).isEqualTo(numKeys);
            assertThat(addressMapTime).isLessThanOrEqualTo(followBackpointersTime);
        } catch(Exception e) {
            System.out.println("**** Exception: " + e);
            // Exception
        } finally {
            rt1.shutdown();
            rt2.shutdown();
            rt3.shutdown();
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
     * @throws Exception
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
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            StreamingMap<String, String> map2 = defaultRT.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
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
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map1, "9", "9");
            transactionalWrite(defaultRT, map2, "10", "10");
            transactionalWrite(defaultRT, map2, "11", "11");

            // Checkpoint S2
            MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
            mcw2.addMap(map2);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map2, "15", "15");

            assertThat(map1).hasSize(sizeMap1);
            assertThat(map2).hasSize(sizeMap2);

            // Trim on the lower address (@5)
            defaultRT.getAddressSpaceView().prefixTrim(minCheckpointAddress);

            // New runtime read s1, read s2 (from checkpoint)
            Map<String, String> map1rt = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            Map<String, String> map2rt = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamName("stream2")
                    .open();

            assertThat(map1rt.size()).isEqualTo(sizeMap1);
            assertThat(map2rt.size()).isEqualTo(sizeMap2);
        } catch (Exception e) {
            fail("Exception thrown", e);
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
     * @throws Exception
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
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            for(int i=0; i<batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 1
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(writeRuntime, "author");

            for(int i=batchWrite; i<batchWrite + batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 2
            MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
            mcw2.addMap(map1);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(writeRuntime, "author");

            assertThat(map1).hasSize(batchWrite+batchWrite);

            // Trim below lower checkpoint
            writeRuntime.getAddressSpaceView().prefixTrim(new Token(minCheckpointAddress.getEpoch(), trimAddress));

            // New runtime
            Map<String, String> map1rt = readRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {
                    })
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
        } catch (Exception e) {
            fail("Exception thrown", e);
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
            CheckpointWriter cpw = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(stream1),
                    "checkpointer-test", mapA);
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress));

            // Start checkpoint with snapshot time 9 for mapB
            CheckpointWriter cpwB = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(stream2),
                    "checkpointer-test", mapB);
            cpwB.appendCheckpoint(new Token(0, snapshotAddress));

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
            StreamAddressSpace addressSpaceA = runtimeRestart.getAddressSpaceView().getLogAddressSpace()
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(stream1));

            // Verify address space and trim mark is properly set for the given stream (should be 7 which  is the start log address
            // for the existing checkpoint)
            assertThat(addressSpaceA.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceA.getAddressMap().getLongCardinality()).isEqualTo(insertions);

            // Fetch Address Space for the given stream S2
            StreamAddressSpace addressSpaceB = runtimeRestart.getAddressSpaceView().getLogAddressSpace()
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(stream2));

            // Verify address space and trim mark is properly set for the given stream (should be 7 which  is the start log address
            // for the existing checkpoint)
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceB.getAddressMap().getLongCardinality()).isEqualTo(insertionsB);

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
     * @throws Exception
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
            CheckpointWriter cpw = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamNameA),
                    "checkpoint-test", mapA);
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress));

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
            StreamAddressSpace addressSpaceA = runtimeRestart.getAddressSpaceView().getLogAddressSpace()
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameA));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceA.getTrimMark()).isEqualTo(snapshotAddress);
            assertThat(addressSpaceA.getAddressMap().getLongCardinality()).isEqualTo(insertions);

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
     * @throws Exception
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
            CheckpointWriter cpw = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamNameA),
                    "checkpointer-test", mapA);
            Token cpAddress = cpw.appendCheckpoint(new Token(0, snapshotAddress - 1));

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
            StreamAddressSpace addressSpaceB = rt2.getAddressSpaceView().getLogAddressSpace()
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameB));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(Address.NON_EXIST);
            assertThat(addressSpaceB.getAddressMap().getLongCardinality()).isEqualTo(insertions);

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
            addressSpaceB = runtimeRestart.getAddressSpaceView().getLogAddressSpace()
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID(streamNameB));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpaceB.getTrimMark()).isEqualTo(Address.NON_EXIST);
            assertThat(addressSpaceB.getAddressMap().getLongCardinality()).isEqualTo(insertions);

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
            CheckpointWriter cpwA = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamA),
                    "checkpointer-Test", mA);
            Token cpTokenA = cpwA.appendCheckpoint();

            // Start a CheckpointWriter for streamB
            CheckpointWriter cpwB = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamB),
                    "checkpointer-Test", mB);
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
     * @throws Exception
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
            CheckpointWriter cpwA = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamA),
                    "checkpointer-Test", map);
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
     * @throws Exception
     */
    @Test
    public void testCheckpointEmptyMapAndSequencerFailover() throws Exception {
        testCheckpointEmptyMapWithSequencerFailover(false);
    }

    /**
     * This test is similar to the previous, but enforces the hole from a logreader, as logreader hole fill
     * does not contain stream information to build address map.
     * @throws Exception
     */
    @Test
    public void testCheckpointEmptyMapWithReaderHoleAndSequencerFailover() throws Exception {
        testCheckpointEmptyMapWithSequencerFailover(true);
    }

    private void testCheckpointEmptyMapWithSequencerFailover(boolean readerHole) throws Exception {
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
                // Generate hole from a logreader
                runtime.getSequencerView().next(streamID).getToken();
                assertThat(mA.size()).isEqualTo(0);
                extraEntry = 1;
            }

            // Start a CheckpointWriter for streamA
            CheckpointWriter cpwA = new CheckpointWriter(runtime, CorfuRuntime.getStreamID(streamA),
                    "checkpointer-Test", mA);
            Token cpTokenA = cpwA.appendCheckpoint();

            // Verify Address Maps from Log Unit and Sequencer Tails (first node)
            StreamsAddressResponse logUnitResponse = runtime.getAddressSpaceView().getLogAddressSpace();
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
            logUnitResponse = runtime.getAddressSpaceView().getLogAddressSpace();
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
