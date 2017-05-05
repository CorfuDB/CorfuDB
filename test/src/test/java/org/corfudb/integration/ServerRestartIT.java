package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

        // Keep this print at all times to reproduce any failed test.
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        final Random rand = new Random(SEED);
        System.out.println("SEED = " + SEED);

        // Runs the corfu server. Expect slight delay until server is running.
        Process corfuServerProcess = runCorfuServer(corfuSingleNodeHost, corfuSingleNodePort);

        // List of runtimes to free resources when not needed.
        List<CorfuRuntime> runtimeList = new ArrayList<>();

        List<Map<String, Integer>> smrMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            CorfuRuntime runtime = createDefaultRuntime();
            runtimeList.add(runtime);
            smrMapList.add(createMap(runtime, Integer.toString(i)));
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
                        CorfuRuntime runtime = createDefaultRuntime();
                        runtimeList.add(runtime);
                        smrMapList.add(createMap(runtime, Integer.toString(j)));
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

    private String getRestartStateRecord(int iteration, boolean serverRestart, boolean clientRestart) {
        return "[" + iteration + "]: ServerRestart=" + serverRestart + ", ClientRestart=" + clientRestart;
    }

    private String getMapStateRecord(int iteration, List<Map<String, Integer>> mapStateList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[" + iteration + "]: Map State :\n");
        for (int i = 0; i < mapStateList.size(); i ++){
            sb.append("map#" + i + " map = " + mapStateList.get(i).toString() + "\n");
        }
        return sb.toString();
    }

    private String getMapInsertion(int iteration, int streamId, String key, int value) {
        return "[" + iteration + "]: Map put => streamId=" + streamId + " key=" + key + " value=" + value;
    }
}
