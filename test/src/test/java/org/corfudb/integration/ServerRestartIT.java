package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.Index;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.StringIndexer;
import org.corfudb.runtime.collections.StringMultiIndexer;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

/**
 * Tests the recovery of the Corfu instance.
 * WARNING: These tests kill all existing corfu instances on the node to run
 * fresh servers.
 * Created by zlokhandwala on 4/25/17.
 */
public class ServerRestartIT extends AbstractIT {

    // Total number of iterations of randomized failovers.
    private static int ITERATIONS;
    // Percentage of Client restarts.
    private static int CLIENT_RESTART_PERCENTAGE;
    // Percentage of Server restarts.
    private static int SERVER_RESTART_PERCENTAGE;

    private static String corfuSingleNodeHost;
    private static int corfuSingleNodePort;

    @Before
    public void loadProperties() {
        ITERATIONS = Integer.parseInt((String) PROPERTIES.get("RandomizedRecoveryIterations"));
        CLIENT_RESTART_PERCENTAGE = Integer.parseInt((String) PROPERTIES.get("ClientRestartPercentage"));
        SERVER_RESTART_PERCENTAGE = Integer.parseInt((String) PROPERTIES.get("ServerRestartPercentage"));
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
        corfuSingleNodePort = Integer.parseInt((String) PROPERTIES.get("corfuSingleNodePort"));
    }

    private Process runCorfuServer() throws IOException {
        return new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, corfuSingleNodePort))
                .runServer();
    }

    private Random getRandomNumberGenerator() {
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        // Keep this print at all times to reproduce any failed test.
        testStatus += "SEED=" + Long.toHexString(SEED);
        return new Random(SEED);
    }

    /**
     * Randomized tests with mixed client and server failovers.
     *
     * @throws Exception error
     */
    @Test
    public void testRandomizedRecovery() throws Exception {

        // Total percentage.
        final int TOTAL_PERCENTAGE = 100;

        // Number of maps or streams to test recovery on.
        final int MAPS = 3;
        // Number of insertions in map in each iteration.
        final int INSERTIONS = 100;
        // Number of keys to be used throughout the test in each map.
        final int KEYS = 20;

        // Logs the server and client state in each iteration with the
        // maps used and keys and values inserted in each iteration.
        final boolean TEST_SEQUENCE_LOGGING = false;
        final File testSequenceLogFile = new File(TEST_SEQUENCE_LOG_PATH);
        if (!testSequenceLogFile.exists()) {
            testSequenceLogFile.createNewFile();
        }

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final Random rand = getRandomNumberGenerator();

        // Runs the corfu server. Expect slight delay until server is running.
        Process corfuServerProcess = runCorfuServer();

        // List of runtimes to free resources when not needed.
        List<CorfuRuntime> runtimeList = new ArrayList<>();

        for (int i = 0; i < MAPS; i++) {
            final int ii = i;
            Future<Boolean> future = executorService.submit(() -> {
                CorfuRuntime runtime = createDefaultRuntime();
                runtimeList.add(runtime);
                this.<String, Integer>createCorfuTable(runtime, Integer.toString(ii), table -> {
                    //NO-OP
                });
                return true;
            });
            future.get(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS);
        }
        List<Map<String, Integer>> expectedMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            expectedMapList.add(new HashMap<>());
        }

        try (final FileOutputStream fos = new FileOutputStream(testSequenceLogFile)) {

            for (int i = 0; i < ITERATIONS; i++) {

                boolean serverRestart = rand.nextInt(TOTAL_PERCENTAGE) < SERVER_RESTART_PERCENTAGE;
                boolean clientRestart = rand.nextInt(TOTAL_PERCENTAGE) < CLIENT_RESTART_PERCENTAGE;

                if (TEST_SEQUENCE_LOGGING) {
                    fos.write(getRestartStateRecord(i, serverRestart, clientRestart).getBytes());
                }

                List<PersistentCorfuTable<String, Integer>> smrMapList = new ArrayList<>();
                if (clientRestart) {
                    runtimeList.parallelStream().forEach(CorfuRuntime::shutdown);
                    for (int j = 0; j < MAPS; j++) {
                        final int jj = j;
                        Future<Boolean> future = executorService.submit(() -> {
                            CorfuRuntime runtime = createDefaultRuntime();
                            runtimeList.add(runtime);
                            smrMapList.add(createCorfuTableUnsafe(runtime, Integer.toString(jj)));
                            return true;
                        });
                        future.get(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS);
                    }
                }

                // Table assertions
                while (true) {
                    try {
                        if (i != 0) {
                            for (int j = 0; j < MAPS; j++) {
                                assertThat(smrMapList.get(j).entryStream())
                                        .containsExactlyInAnyOrderElementsOf(expectedMapList.get(j).entrySet());
                            }
                        }
                        break;
                    } catch (NetworkException ne) {
                        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
                    }
                }

                // Map insertions
                for (int j = 0; j < INSERTIONS; j++) {
                    int value = rand.nextInt();
                    int map = rand.nextInt(MAPS);
                    String key = Integer.toString(rand.nextInt(KEYS));

                    smrMapList.get(map).insert(key, value);
                    expectedMapList.get(map).put(key, value);

                    if (TEST_SEQUENCE_LOGGING) {
                        fos.write(getMapInsertion(i, map, key, value).getBytes());
                    }
                }

                if (TEST_SEQUENCE_LOGGING) {
                    fos.write(getMapStateRecord(i, expectedMapList).getBytes());
                }

                if (serverRestart) {
                    assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
                    corfuServerProcess = runCorfuServer();
                }
            }

            assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
        } catch (Exception e) {
            throw e;
        }

        runtimeList.forEach(CorfuRuntime::shutdown);
    }


    /**
     * Test server failure and recovery on a transaction-based client (with nested transactions).
     *
     * @throws Exception
     */
    // TODO(Zach): PersistentCorfuTable does not have a compute method
    /*
    @Test
    public void testSingleNodeRecoveryTransactionalClientNested() throws Exception {
        runSingleNodeRecoveryTransactionalClient(true);
    }
    */

    /**
     * Test server failure and recovery on a transaction-based client (non-nested transactions).
     *
     * @throws Exception error
     */
    // TODO(Zach): PersistentCorfuTable does not have a compute method
    /*
    @Test
    public void testSingleNodeRecoveryTransactionalClient() throws Exception {
        runSingleNodeRecoveryTransactionalClient(false);
    }
    */

    // TODO(Zach): PersistentCorfuTable does not have a compute method
    /*
    private void runSingleNodeRecoveryTransactionalClient(boolean nested) throws Exception {

        // Total number of maps (streams) to write to.
        final int MAPS = 5;
        final int MAX_LIMIT_KEY_RANGE_PRE_SHUTDOWN = 20;
        final int MIN_LIMIT_KEY_RANGE_DURING_SHUTDOWN = 30;
        final int MAX_LIMIT_KEY_RANGE_DURING_SHUTDOWN = 60;
        final int MIN_LIMIT_KEY_RANGE_POST_SHUTDOWN = 100;
        final int MAX_LIMIT_KEY_RANGE_POST_SHUTDOWN = 200;
        final int CLIENT_DELAY_POST_SHUTDOWN = 50;

        final int CORFU_SERVER_DOWN_TIME = 4000;

        final Random rand = getRandomNumberGenerator();

        // Run CORFU Server. Expect slight delay until server is running.
        final Process corfuServerProcess = runCorfuServer();
        assertThat(corfuServerProcess.isAlive()).isTrue();

        // Initialize Client: Create Runtime (Client)
        runtime = createDefaultRuntime();

        // Create Maps
        List<Map<String, Integer>> smrMapList = new ArrayList<>();
        List<Map<String, Integer>> expectedMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            smrMapList.add(createMap(runtime, Integer.toString(i)));
            expectedMapList.add(new HashMap<>());
        }

        // Execute Transactions (while Corfu Server RUNNING)
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(executeTransaction(runtime, smrMapList, expectedMapList, 0,
                    MAX_LIMIT_KEY_RANGE_PRE_SHUTDOWN, nested, rand)).isTrue();
        }

        // ShutDown (STOP) CORFU Server
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();

        // Schedule offline transactions, first one should be stuck and will eventually succeed
        // on reconnect
        ScheduledThreadPoolExecutor offline = new ScheduledThreadPoolExecutor(1);
        ScheduledFuture<Boolean> offlineTransactionsSucceeded = offline.schedule(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                boolean txState = executeTransaction(runtime, smrMapList, expectedMapList,
                        MIN_LIMIT_KEY_RANGE_DURING_SHUTDOWN, MAX_LIMIT_KEY_RANGE_DURING_SHUTDOWN,
                        nested, rand);

                if (!txState) {
                    return false;
                }
            }

            return true;
        }, CLIENT_DELAY_POST_SHUTDOWN, TimeUnit.MILLISECONDS);
        offline.shutdown();

        Thread.sleep(CORFU_SERVER_DOWN_TIME);

        // Restart Corfu Server
        Process corfuServerProcessRestart = runCorfuServer();
        offline.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        // Block until server is ready.
        runtime.invalidateLayout();
        runtime.getLayoutView().getLayout();

        // Execute Transactions (once Corfu Server was restarted)
        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(executeTransaction(runtime, smrMapList, expectedMapList,
                    MIN_LIMIT_KEY_RANGE_POST_SHUTDOWN, MAX_LIMIT_KEY_RANGE_POST_SHUTDOWN,
                    nested, rand)).isTrue();
        }

        // Verify Correctness
        // Note: by triggering this from a separate thread we guarantee that we can catch any potential problems
        // related to transactional contexts not being removed for current thread.
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        ScheduledFuture<Boolean> future = exec.schedule(() -> {

            for (int i = 0; i < expectedMapList.size(); i++) {
                Map<String, Integer> expectedMap = expectedMapList.get(i);
                Map<String, Integer> smrMap = smrMapList.get(i);
                if (!expectedMap.equals(smrMap)) {
                    return false;
                }
            }
            return true;
        }, CLIENT_DELAY_POST_SHUTDOWN, TimeUnit.MILLISECONDS);

        // Wait for Executor to Finish
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Data Correctness Validation
        assertThat(future.get()).isTrue();
        assertThat(offlineTransactionsSucceeded.get()).isTrue();

        // ShutDown the server before exiting
        assertThat(shutdownCorfuServer(corfuServerProcessRestart)).isTrue();
    }
    */

    // TODO(Zach): PersistentCorfuTable does not have a compute method
    /*
    private boolean executeTransaction(CorfuRuntime runtime, List<Map<String, Integer>> smrMapList,
                                       List<Map<String, Integer>> expectedMapList, int minKeyRange,
                                       int maxKeyRange, boolean nested, Random rand) {

        // Number of insertions in map in each iteration.
        final int INSERTIONS = 100;
        boolean retry = true;

        boolean success = false;

        while (retry) {
            try {
                retry = false;
                // Start Transaction
                runtime.getObjectsView().TXBegin();

                // Map insertions
                for (int j = 0; j < INSERTIONS; j++) {
                    int value = rand.nextInt();
                    String key = Integer.toString(ThreadLocalRandom.current().nextInt(minKeyRange, maxKeyRange + 1));

                    for (int i = 0; i < smrMapList.size(); i++) {
                        if (nested) {
                            smrMapList.get(i).compute(key, (k, v) -> (v == null) ? value : v);
                            expectedMapList.get(i).compute(key, (k, v) -> (v == null) ? value : v);
                        } else {
                            smrMapList.get(i).put(key, value);
                            expectedMapList.get(i).put(key, value);
                        }
                    }
                }

                // End Transaction
                runtime.getObjectsView().TXEnd();

                success = true;

                for (int i = 0; i < smrMapList.size(); i++) {
                    assertThat(expectedMapList.get(i)).isEqualTo(smrMapList.get(i));
                }
            } catch (Exception e) {
                assertThat(e).isExactlyInstanceOf(TransactionAbortedException.class);
                // If a transaction is aborted from a cause different from NetworkException (which is the relevant to
                // this integration test, retry)
                if (((TransactionAbortedException) e).getAbortCause() != AbortCause.NETWORK) {
                    retry = true;
                }
            }
        }

        return success;
    }
    */

    private String getRestartStateRecord(int iteration, boolean serverRestart, boolean clientRestart) {
        return "[" + iteration + "]: ServerRestart=" + serverRestart + ", ClientRestart=" + clientRestart;
    }

    private String getMapStateRecord(int iteration, List<Map<String, Integer>> mapStateList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(iteration).append("]: Map State :\n");
        for (int i = 0; i < mapStateList.size(); i++) {
            sb.append("map#")
                    .append(i)
                    .append(" map = ").
                    append(mapStateList.get(i).toString())
                    .append("\n");
        }
        return sb.toString();
    }

    private String getMapInsertion(int iteration, int streamId, String key, int value) {
        return "[" + iteration + "]: Map put => streamId=" + streamId + " key=" + key + " value=" + value;
    }

    /**
     * Start the server, create 2 maps A and B.
     * Putting 10 entries 0 -> 9 in mapA.
     * Putting 10 entries 10 -> 19 in mapB.
     * Restarting the server. On restart, the management server should run the
     * FastSMRLoader and reset the SequencerServer with the stream tails.
     * Now request for tokens in both streamA and streamB and assert on the correct backpointers.
     *
     * @throws Exception error
     */
    @Test
    public void sequencerTailsRecoveryTest() throws Exception {

        Process corfuServerProcess = runCorfuServer();
        final int insertions = 10;
        UUID streamNameA = CorfuRuntime.getStreamID("mapA");
        UUID streamNameB = CorfuRuntime.getStreamID("mapB");

        CorfuRuntime runtime = createDefaultRuntime();

        this.<String, Integer>createCorfuTable(runtime, "mapA", mapA -> {
            this.<String, Integer>createCorfuTable(runtime, "mapB", mapB -> {
                for (int i = 0; i < insertions; i++) {
                    mapA.insert(Integer.toString(i), i);
                }
                for (int i = 0; i < insertions; i++) {
                    mapB.insert(Integer.toString(i), i);
                }

                // Now the stream tails are: mapA=9, mapB=19
                final int newMapAStreamTail = 9;
                final int newMapBStreamTail = 19;
                final int newGlobalTail = 19;

                restartServer(runtime, DEFAULT_ENDPOINT);

                TokenResponse tokenResponseA = runtime.getSequencerView().next(streamNameA);
                TokenResponse tokenResponseB = runtime.getSequencerView().next(streamNameB);

                assertThat(tokenResponseA.getToken().getSequence()).isEqualTo(newGlobalTail + 1);
                assertThat(tokenResponseA.getBackpointerMap().get(streamNameA))
                        .isEqualTo(newMapAStreamTail);

                assertThat(tokenResponseB.getToken().getSequence()).isEqualTo(newGlobalTail + 2);
                assertThat(tokenResponseB.getBackpointerMap().get(streamNameB))
                        .isEqualTo(newMapBStreamTail);

                assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
            });
        });
    }

    /**
     * If a tokenResponse is received in the previous epoch,
     * a write should discard the tokenResponse and throw an exception.
     *
     * @throws Exception error
     */
    @Test
    public void discardTokenReceivedInPreviousEpoch() throws Exception {
        final int timeToWaitInSeconds = 3;

        Process corfuServerProcess = runCorfuServer();
        CorfuRuntime runtime = createDefaultRuntime();

        // wait for this server long enough to start (by requesting token service)
        TokenResponse firsttr = runtime.getSequencerView().next();

        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();

        corfuServerProcess = runCorfuServer();

        runtime.invalidateLayout();
        TokenResponse tr = runtime.getSequencerView().next();
        assertThat(tr.getEpoch()).isEqualTo(1);

        // Force the token response to have epoch = 0, to simulate a request received in previous epoch
        Token staleToken = new Token(tr.getEpoch() - 1, tr.getSequence());
        TokenResponse mockTr = new TokenResponse(staleToken, Collections.emptyMap());

        byte[] testPayload = "hello world".getBytes();

        // Should succeed. internally, it will refresh the token.
        CompletableFuture<Boolean> cf = CFUtils.within(CompletableFuture.supplyAsync(() -> {
            runtime.getAddressSpaceView().write(mockTr, testPayload);
            return true;
        }), Duration.ofSeconds(timeToWaitInSeconds));

        try {
            cf.get();
        } catch (Exception e) {
            assertThat(e.getCause()).isNotInstanceOf(TimeoutException.class);
            assertThat(e.getCause()).isInstanceOf(StaleTokenException.class);
        }

        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
    }

    private void createTable(
            CorfuRuntime corfuRuntime, Index.Registry indexer,
            ThrowableConsumer<PersistentCorfuTable<String, String>> action) throws Exception {

        try(PersistentCorfuTable<String, String> table = corfuRuntime.getObjectsView().build()
                .setTypeToken(PersistentCorfuTable.<String, String>getTableType())
                .setArguments(indexer)
                .setStreamName("test")
                .open()) {
            action.accept(table);
        }
    }

    private <T> Collection<T> toCollection(@Nonnull Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
    }

    /**
     * Data generation. First 1000 entries are written to the table.
     * The log is then check-pointed and trimmed. The server is then restarted.
     * We now start 2 clients rt2 and rt3, both of which should recreate the log and also
     * reconstruct the indices.
     * Finally we assert the reconstructed indices.
     *
     * @throws Exception error
     */
    @Test
    public void testCorfuTableIndexReconstruction() throws Exception {

        // Start server
        final Process corfuProcess = runCorfuServer();

        // Write 1000 entries.
        CorfuRuntime runtime1 = createRuntimeWithCache();
        createTable(runtime1, new StringIndexer(), corfuTable1 -> {
            final int num = 1000;
            for (int i = 0; i < num; i++) {
                corfuTable1.insert(Integer.toString(i), Integer.toString(i));
            }

            // Checkpoint and trim the log.
            MultiCheckpointWriter<PersistentCorfuTable<String, String>> mcw = new MultiCheckpointWriter<>();
            mcw.addMap(corfuTable1);
            Token trimMark = mcw.appendCheckpoints(runtime1, "author");

            final String indexKey = "9";

            Collection<Map.Entry<String, String>> c1a =
                    toCollection(corfuTable1.getByIndex(StringIndexer.BY_FIRST_LETTER, indexKey));
            Collection<Map.Entry<String, String>> c1b =
                    toCollection(corfuTable1.getByIndex(StringIndexer.BY_VALUE, indexKey));
            runtime1.getAddressSpaceView().prefixTrim(trimMark);
            runtime1.getAddressSpaceView().invalidateClientCache();
            runtime1.getAddressSpaceView().invalidateServerCaches();
            runtime1.getAddressSpaceView().gc();

            // Restart the corfu server.
            assertThat(shutdownCorfuServer(corfuProcess)).isTrue();
            Process corfuServer2 = runCorfuServer();

            // Start a new client and verify the index.
            CorfuRuntime runtime2 = createRuntimeWithCache();
            createTable(runtime2, new StringIndexer(), corfuTable2 -> {

                Collection<Map.Entry<String, String>> c2 =
                        toCollection(corfuTable2.getByIndex(StringIndexer.BY_FIRST_LETTER, indexKey));
                assertThat(c1a).hasSameSizeAs(c2);
                assertThat(c1a.containsAll(c2)).isTrue();

                // Start a new client with cache disabled and fast object loading disabled.
                CorfuRuntime runtime3 = createDefaultRuntime();
                createTable(runtime3, new StringIndexer(), corfuTable3 -> {
                    Collection<Map.Entry<String, String>> c3 =
                            toCollection(corfuTable3.getByIndex(StringIndexer.BY_VALUE, indexKey));
                    assertThat(c1b).hasSameSizeAs(c3);
                    assertThat(c1b.containsAll(c3)).isTrue();
                });
            });

            // Stop the corfu server.
            assertThat(shutdownCorfuServer(corfuServer2)).isTrue();
        });
    }

    /**
     * This test has the following steps in order to verify the multi index reconstruction:
     * 1) Writes 1000 entries to the table
     * 2) Checkpoints and trims
     * 3) Shuts down and restarts the corfu server
     * 4) Starts a client with cache and fast loading and verify multi index
     * 5) Starts a client without cache and fast loading and verify multi index
     *
     * @throws Exception error
     */
    @Test
    public void testCorfuTableMultiIndexReconstruction() throws Exception {
        // Start server
        final Process corfuProcess = runCorfuServer();

        // Write 1000 entries
        CorfuRuntime runtime1 = createRuntimeWithCache();

        createTable(runtime1, new StringMultiIndexer(), corfuTable1 -> {
            final int numEntries = 1000;
            for (int i = 0; i < numEntries; i++) {
                String value = "tag" + i + " tag" + (i + 1) + " tag" + (i + 2);
                String key = "key" + i;
                corfuTable1.insert(key, value);
            }

            // Checkpoint and trim
            MultiCheckpointWriter<PersistentCorfuTable<String, String>> multiCheckpointWriter = new MultiCheckpointWriter<>();
            multiCheckpointWriter.addMap(corfuTable1);
            Token trimMark = multiCheckpointWriter.appendCheckpoints(runtime1, "Sam.Behnam");

            final String indexKey = "tag666";

            Collection<Map.Entry<String, String>> resultInitial =
                    toCollection(corfuTable1.getByIndex(StringMultiIndexer.BY_EACH_WORD, indexKey));
            runtime1.getAddressSpaceView().prefixTrim(trimMark);
            runtime1.getAddressSpaceView().invalidateClientCache();
            runtime1.getAddressSpaceView().invalidateServerCaches();
            runtime1.getAddressSpaceView().gc();

            //Restart the corfu server
            assertThat(shutdownCorfuServer(corfuProcess)).isTrue();
            Process newCorfuProcess = runCorfuServer();

            // Start a new client and verify the multi index.
            CorfuRuntime runtime2 = createRuntimeWithCache();
            createTable(runtime2, new StringMultiIndexer(), corfuTable2 -> {
                Collection<Map.Entry<String, String>> resultAfterRestart =
                        toCollection(corfuTable2.getByIndex(StringMultiIndexer.BY_EACH_WORD, indexKey));
                assertThat(resultAfterRestart).hasSameSizeAs(resultInitial);
                assertThat(resultAfterRestart.containsAll(resultInitial)).isTrue();

                // Start a new client with cache and fast object loading disabled and verify multi index.
                CorfuRuntime runtime3 = createDefaultRuntime();

                createTable(runtime3, new StringMultiIndexer(), corfuTable3 -> {
                    Collection<Map.Entry<String, String>> resultDisabledCacheAndFasLoader =
                            toCollection(corfuTable3.getByIndex(StringMultiIndexer.BY_EACH_WORD, indexKey));
                    assertThat(resultDisabledCacheAndFasLoader).hasSameSizeAs(resultInitial);
                    assertThat(resultDisabledCacheAndFasLoader.containsAll(resultInitial)).isTrue();
                });
            });

            // Stop the corfu server
            assertThat(shutdownCorfuServer(newCorfuProcess)).isTrue();
        });
    }

    /**
     * This test verifies that a stream is rebuilt from the latest checkpoint (based on the snapshot it covers)
     * even though an older checkpoint (lowest snapshot) appears later in the stream.
     * <p>
     * It also verifies that behaviour is kept the same after the node is restarted and both checkpoints
     * still exist.
     * <p>
     * 1. Write 25 entries to stream A.
     * 2. Start a checkpoint (CP2) at snapshot 15, complete it.
     * 3. Start a checkpoint (CP1) at snapshot 10, complete it.
     * 4. Trim on token for CP2 (snapshot = 15).
     * 5. New runtime instantiate stream A (do a mutation to force to load from checkpoint).
     * 6. Restart the server
     * 7. Instantiate map again.
     * <p>
     * It is expected in all cases that maps are successfully rebuilt, all entries present
     * and no TrimmedException is thrown on access.
     */
    @Test
    public void testUnorderedCheckpointsAndRestartServer() throws Exception {
        final int numEntries = 25;
        final int snapshotAddress1 = 10;
        final int snapshotAddress2 = 15;

        // Start server
        final Process corfuProcess = runCorfuServer();

        CorfuRuntime r = createRuntimeWithCache();

        // Open table.
        createTable(r, new StringMultiIndexer(), corfuTable1 -> {
            // (1) Write 25 Entries
            for (int i = 0; i < numEntries; i++) {
                corfuTable1.insert(String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint Writer 2
            CheckpointWriter<PersistentCorfuTable<String, String>> cpw2 =
                    new CheckpointWriter<>(r, CorfuRuntime.getStreamID("test"), "checkpointer-2", corfuTable1);
            Token cp2Token = cpw2.appendCheckpoint(new Token(0, snapshotAddress2 - 1), Optional.empty());

            // Checkpoint Writer 1
            CheckpointWriter<PersistentCorfuTable<String, String>> cpw1 =
                    new CheckpointWriter<>(r, CorfuRuntime.getStreamID("test"), "checkpointer-1", corfuTable1);
            cpw1.appendCheckpoint(new Token(0, snapshotAddress1 - 1), Optional.empty());

            // Trim @snapshotAddress=15 (Checkpoint Writer 2)
            r.getAddressSpaceView().prefixTrim(cp2Token);

            // Start a new Runtime
            CorfuRuntime rt2 = createRuntimeWithCache();
            createTable(rt2, new StringMultiIndexer(), corfuTable2 -> {
                rt2.getObjectsView().TXBegin();
                corfuTable2.insert("a", "a");
                rt2.getObjectsView().TXEnd();

                assertThat(corfuTable2.size()).isEqualTo(numEntries + 1);

                //Restart the corfu server
                assertThat(shutdownCorfuServer(corfuProcess)).isTrue();
                Process corfuProcess2 = runCorfuServer();

                // Start a new Runtime
                CorfuRuntime rt3 = createRuntimeWithCache();
                createTable(rt3, new StringMultiIndexer(), corfuTable3 -> {
                    rt3.getObjectsView().TXBegin();
                    corfuTable3.insert("b", "b");
                    rt3.getObjectsView().TXEnd();

                    assertThat(corfuTable3.size()).isEqualTo(numEntries + 2);
                });

                shutdownCorfuServer(corfuProcess2);
            });
        });
    }

    /**
     * Check streamAddressSpace rebuilt from log unit
     * when stream has been previously checkpointed and trimmed.
     **/
    @Test
    public void checkStreamAddressSpaceRebuiltWithTrim() throws Exception {
        final int numEntries = 10;

        final List<Long> expectedAddresses = new ArrayList<>(
                Arrays.asList(14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L));

        // Start server
        Process corfuProcess = runCorfuServer();

        // Start runtime
        CorfuRuntime r = createDefaultRuntime();

        // Open table
        createTable(r, new StringMultiIndexer(), table -> {
            // Write numEntries to table
            for (int i = 0; i < numEntries; i++) {
                table.insert(String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint
            MultiCheckpointWriter<PersistentCorfuTable<String, String>> cpw = new MultiCheckpointWriter<>();
            cpw.addMap(table);
            Token cpAddress = cpw.appendCheckpoints(r, "cp-test");

            // Trim the log
            r.getAddressSpaceView().prefixTrim(cpAddress);
            r.getAddressSpaceView().gc();
            r.getAddressSpaceView().invalidateServerCaches();
            r.getAddressSpaceView().invalidateClientCache();

            // Write another numEntries to table
            for (int i = numEntries; i < numEntries * 2; i++) {
                table.insert(String.valueOf(i), String.valueOf(i));
            }

            //Restart the corfu server
            assertThat(shutdownCorfuServer(corfuProcess)).isTrue();
            Process corfuProcess2 = runCorfuServer();

            // Start NEW runtime
            CorfuRuntime runtimeRestart = createDefaultRuntime();

            // Fetch Address Space for the given stream
            StreamAddressSpace addressSpace = Utils.getLogAddressSpace(runtimeRestart
                            .getLayoutView().getRuntimeLayout())
                    .getAddressMap()
                    .get(CorfuRuntime.getStreamID("test"));

            // Verify address space and trim mark is properly set for the given stream.
            assertThat(addressSpace.getTrimMark()).isEqualTo(cpAddress.getSequence());

            assertThat(addressSpace.size()).isEqualTo(expectedAddresses.size());
            expectedAddresses.forEach(address -> assertThat(addressSpace.contains(address)).isTrue());

            shutdownCorfuServer(corfuProcess2);
        });
    }
}
