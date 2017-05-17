package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the recovery of the Corfu instance.
 * WARNING: These tests kill all existing corfu instances on the node to run
 * fresh servers.
 * Created by zlokhandwala on 4/25/17.
 */
public class ServerRestartIT extends AbstractIT {

    // Total number of iterations of randomized failovers.
    static int ITERATIONS;
    // Percentage of Client restarts.
    static int CLIENT_RESTART_PERCENTAGE;
    // Percentage of Server restarts.
    static int SERVER_RESTART_PERCENTAGE;

    static String corfuSingleNodeHost;
    static int corfuSingleNodePort;

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


    /**
     * Randomized tests with mixed client and server failovers.
     *
     * @throws Exception
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
        final boolean TEST_SEQUENCE_LOGGING = true;
        final File testSequenceLogFile = new File(TEST_SEQUENCE_LOG_PATH);
        if (!testSequenceLogFile.exists()) {
            testSequenceLogFile.createNewFile();
        }

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Keep this print at all times to reproduce any failed test.
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        final Random rand = new Random(SEED);
        System.out.println("SEED = " + SEED);

        // Runs the corfu server. Expect slight delay until server is running.
        Process corfuServerProcess = runCorfuServer();

        // List of runtimes to free resources when not needed.
        List<CorfuRuntime> runtimeList = new ArrayList<>();

        List<Map<String, Integer>> smrMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            final int ii = i;
            Future<Boolean> future = executorService.submit(() -> {
                CorfuRuntime runtime = createDefaultRuntime();
                runtimeList.add(runtime);
                smrMapList.add(createMap(runtime, Integer.toString(ii)));
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

                System.out.println("Iteration #" + i);
                boolean serverRestart = rand.nextInt(TOTAL_PERCENTAGE) < SERVER_RESTART_PERCENTAGE;
                boolean clientRestart = rand.nextInt(TOTAL_PERCENTAGE) < CLIENT_RESTART_PERCENTAGE;

                if (TEST_SEQUENCE_LOGGING) {
                    fos.write(getRestartStateRecord(i, serverRestart, clientRestart).getBytes());
                }

                if (clientRestart) {
                    smrMapList.clear();
                    runtimeList.forEach(CorfuRuntime::shutdown);
                    for (int j = 0; j < MAPS; j++) {
                        final int jj = j;
                        Future<Boolean> future = executorService.submit(() -> {
                            CorfuRuntime runtime = createDefaultRuntime();
                            runtimeList.add(runtime);
                            smrMapList.add(createMap(runtime, Integer.toString(jj)));
                            return true;
                        });
                        future.get(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS);
                    }
                }

                // Map assertions
                while (true) {
                    try {
                        if (i != 0) {
                            for (int j = 0; j < MAPS; j++) {
                                assertThat(smrMapList.get(j)).isEqualTo(expectedMapList.get(j));
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

                    smrMapList.get(map).put(key, value);
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
    }


    /**
     * Test server failure and recovery on a transaction-based client (with nested transactions).
     *
     * @throws Exception
     */
    @Test
    public void testSingleNodeRecoveryTransactionalClientNested() throws Exception {
        try {
            runSingleNodeRecoveryTransactionalClient(true);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Test server failure and recovery on a transaction-based client (non-nested transactions).
     *
     * @throws Exception
     */
    @Test
    public void testSingleNodeRecoveryTransactionalClient() throws Exception {
        try {
            runSingleNodeRecoveryTransactionalClient(false);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void runSingleNodeRecoveryTransactionalClient(boolean nested) throws Exception {
        try {
            // Total number of maps (streams) to write to.
            final int MAPS = 5;
            final int MAX_LIMIT_KEY_RANGE_PRE_SHUTDOWN = 20;
            final int MIN_LIMIT_KEY_RANGE_DURING_SHUTDOWN = 30;
            final int MAX_LIMIT_KEY_RANGE_DURING_SHUTDOWN = 60;
            final int MIN_LIMIT_KEY_RANGE_POST_SHUTDOWN = 100;
            final int MAX_LIMIT_KEY_RANGE_POST_SHUTDOWN = 200;
            final int CLIENT_DELAY_POST_SHUTDOWN = 50;

            // Run CORFU Server. Expect slight delay until server is running.
            System.out.println("Start Corfu Server");
            final Process corfuServerProcess = runCorfuServer();
            // Delay (time to start)
            Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());
            assertThat(corfuServerProcess.isAlive()).isTrue();

            // Initialize Client: Create Runtime (Client)
            CorfuRuntime runtime = createDefaultRuntime();

            // Create Maps
            List<Map<String, Integer>> smrMapList = new ArrayList<>();
            List<Map<String, Integer>> expectedMapList = new ArrayList<>();
            for (int i = 0; i < MAPS; i++) {
                smrMapList.add(createMap(runtime, Integer.toString(i)));
                Map<String, Integer> expectedMap = new HashMap<>();
                expectedMapList.add(expectedMap);
            }

            // Execute Transactions (while Corfu Server RUNNING)
            for (int i = 0; i < ITERATIONS; i++) {
                assertThat(executeTransaction(runtime, smrMapList, expectedMapList, 0,
                        MAX_LIMIT_KEY_RANGE_PRE_SHUTDOWN, nested)).isTrue();
            }

            // ShutDown (STOP) CORFU Server
            System.out.println("Shutdown Corfu Server");
            assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();

            // Execute Transactions (once Corfu Server Shutdown)
            for (int i = 0; i < ITERATIONS; i++) {
                assertThat(executeTransaction(runtime, smrMapList, expectedMapList,
                        MIN_LIMIT_KEY_RANGE_DURING_SHUTDOWN, MAX_LIMIT_KEY_RANGE_DURING_SHUTDOWN, nested)).isFalse();
            }

            // Restart Corfu Server
            System.out.println("Restart Corfu Server");
            Process corfuServerProcessRestart = runCorfuServer();
            // Delay (time to restart)
            Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());
            assertThat(corfuServerProcessRestart.isAlive()).isTrue();

            // Execute Transactions (once Corfu Server was restarted)
            for (int i = 0; i < ITERATIONS; i++) {
                assertThat(executeTransaction(runtime, smrMapList, expectedMapList,
                        MIN_LIMIT_KEY_RANGE_POST_SHUTDOWN, MAX_LIMIT_KEY_RANGE_POST_SHUTDOWN, nested)).isTrue();
            }

            // Verify Correctness
            // Note: by triggering this from a separate thread we guarantee that we can catch any potential problems
            // related to transactional contexts not being removed for current thread.
            ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
            ScheduledFuture<Boolean> future = exec.schedule(new Callable<Boolean>() {
                public Boolean call() {
                    for (int i = 0; i < expectedMapList.size(); i++) {
                        Map<String, Integer> expectedMap = expectedMapList.get(i);
                        for (Map.Entry<String, Integer> entry : expectedMap.entrySet()) {
                            if (smrMapList.get(i).get(entry.getKey()) != null) {
                                if (!(smrMapList.get(i).get(entry.getKey()).equals(entry.getValue().intValue()))) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                    return true;
                }
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

            // ShutDown the server before exiting
            System.out.println("Shutdown Corfu Server");
            assertThat(shutdownCorfuServer(corfuServerProcessRestart)).isTrue();
        } catch (Exception e) {
            throw e;
        }
    }

    private boolean executeTransaction (CorfuRuntime runtime, List<Map<String, Integer>> smrMapList,
                                             List<Map<String, Integer>> expectedMapList, int minKeyRange,
                                             int maxKeyRange, boolean nested) {

        // Number of insertions in map in each iteration.
        final int INSERTIONS = 100;
        boolean retry = true;

        // Keep this print at all times to reproduce any failed test.
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        final Random rand = new Random(SEED);
        System.out.println("SEED = " + SEED);

        boolean success = false;

        while(retry) {
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
                    assertThat(smrMapList.get(i)).isEqualTo(expectedMapList.get(i));
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

    private String getRestartStateRecord(int iteration, boolean serverRestart, boolean clientRestart) {
        return "[" + iteration + "]: ServerRestart=" + serverRestart + ", ClientRestart=" + clientRestart;
    }

    private String getMapStateRecord(int iteration, List<Map<String, Integer>> mapStateList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[" + iteration + "]: Map State :\n");
        for (int i = 0; i < mapStateList.size(); i++) {
            sb.append("map#" + i + " map = " + mapStateList.get(i).toString() + "\n");
        }
        return sb.toString();
    }

    private String getMapInsertion(int iteration, int streamId, String key, int value) {
        return "[" + iteration + "]: Map put => streamId=" + streamId + " key=" + key + " value=" + value;
    }
}


