package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;

import org.junit.Test;

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
public class ServerRestartTest extends AbstractIntegrationTest {

    /**
     * Randomized tests with mixed client and server failovers.
     *
     * @throws Exception
     */
    @Test
    public void testRandomizedRecovery() throws Exception {

        // Total number of iterations of randomized failovers.
        final int ITERATIONS = 20;
        // Total percentage.
        final int TOTAL_PERCENTAGE = 100;
        // Percentage of Client restarts.
        final int CLIENT_RESTART_PERCENTAGE = 70;
        // Percentage of Server restarts.
        final int SERVER_RESTART_PERCENTAGE = 70;

        // Number of maps or streams to test recovery on.
        final int MAPS = 3;
        // Number of insertions in map in each iteration.
        final int INSERTIONS = 100;
        // Number of keys to be used throughout the test in each map.
        final int KEYS = 20;

        final Random randomSeed = new Random();

        final long SEED = randomSeed.nextLong();
        final Random rand = new Random(SEED);

        // Keep this print at all times to reproduce any failed test.
        System.out.println("SEED = " + SEED);

        Process corfuServerProcess = runCorfuServer();

        List<String> testSequenceList = new ArrayList<>();

        List<CorfuRuntime> runtimeList = new ArrayList<>();

        List<Map<String, Integer>> smrMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            CorfuRuntime runtime = createRuntime();
            runtimeList.add(runtime);
            smrMapList.add(createMap(runtime, Integer.toString(i)));
        }
        List<Map<String, Integer>> expectedMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            expectedMapList.add(new HashMap<>());
        }

        try {

            for (int i = 0; i < ITERATIONS; i++) {

                boolean serverRestart = rand.nextInt(TOTAL_PERCENTAGE) < SERVER_RESTART_PERCENTAGE;
                boolean clientRestart = rand.nextInt(TOTAL_PERCENTAGE) < CLIENT_RESTART_PERCENTAGE;

                testSequenceList.add(getRestartStateRecord(i, serverRestart, clientRestart));

                if (clientRestart) {
                    smrMapList.clear();
                    runtimeList.forEach(corfuRuntime -> {
                        corfuRuntime.stop();
                        corfuRuntime.shutdown();
                    });
                    for (int j = 0; j < MAPS; j++) {
                        CorfuRuntime runtime = createRuntime();
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

                    testSequenceList.add(getMapInsertion(i, map, key, value));
                }

                testSequenceList.add(getMapStateRecord(i, expectedMapList));

                if (serverRestart) {
                    shutdownCorfuServer(corfuServerProcess);
                    assert !corfuServerProcess.isAlive();
                    runCorfuServer();
                }
            }

            shutdownCorfuServer(corfuServerProcess);

        } catch (Exception e) {
            testSequenceList.forEach(System.out::println);
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
