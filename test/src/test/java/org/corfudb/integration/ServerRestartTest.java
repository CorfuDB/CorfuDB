package org.corfudb.integration;

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
     * Test recovery with a failed over server and a new client.
     *
     * @throws Exception
     */
    @Test
    public void testRecoveryFailoverServerAndClient() throws Exception {

        // Set up and run server
        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime(), "A");

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);

        // Shut down and assert node is down.
        shutdownCorfuServer(corfuServerProcess);
        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();
        map = createMap(createRuntime(), "A");
        assertThat(map.get("a")).isEqualTo(expectedValue);

        // final destroy
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Test recovery with a new client without server failover.
     *
     * @throws Exception
     */
    @Test
    public void testRecoveryFailoverClient() throws Exception {

        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime(), "A");

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);

        map = createMap(createRuntime(), "A");

        assertThat(map.get("a")).isEqualTo(expectedValue);

        // final destroy
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Test recovery with a failover sequencer but the same client querying.
     *
     * @throws Exception
     */
    @Test
    public void testRecoveryFailoverServer() throws Exception {

        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime(), "A");

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);

        shutdownCorfuServer(corfuServerProcess);

        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();

        while (true) {
            try {
                assertThat(map.get("a")).isEqualTo(expectedValue);
                break;
            } catch (NetworkException ne) {
                Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            }
        }

        // final destroy
        shutdownCorfuServer(corfuServerProcess);
    }

    /**
     * Randomized tests with mixed client and server failovers.
     *
     * @throws Exception
     */
    @Test
    public void testRandomizedRecovery() throws Exception {

        // Total number of iterations of randomized failovers.
        final int ITERATIONS = 10;
        // Total percentage.
        final int TOTAL_PERCENTAGE = 100;
        // Percentage of Client restarts.
        final int CLIENT_RESTART_PERCENTAGE = 70;
        // Percentage of Server restarts.
        final int SERVER_RESTART_PERCENTAGE = 70;

        // Number of maps or streams to test recovery on.
        final int MAPS = 3;
        // Number of insertions in map in each iteration.
        final int INSERTIONS = 10;
        // Number of keys to be used throughout the test in each map.
        final int KEYS = 3;

        final Random rand = new Random();

        Process corfuServerProcess = runCorfuServer();
        List<Map<String, Integer>> smrMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            smrMapList.add(createMap(createRuntime(), Integer.toString(i)));
        }
        List<Map<String, Integer>> expectedMapList = new ArrayList<>();
        for (int i = 0; i < MAPS; i++) {
            expectedMapList.add(new HashMap<>());
        }

        for (int i = 0; i < ITERATIONS; i++) {

            boolean serverRestart = rand.nextInt(TOTAL_PERCENTAGE) < SERVER_RESTART_PERCENTAGE;
            boolean clientRestart = rand.nextInt(TOTAL_PERCENTAGE) < CLIENT_RESTART_PERCENTAGE;

            if (clientRestart) {
                smrMapList.clear();
                for (int j = 0; j < MAPS; j++) {
                    smrMapList.add(createMap(createRuntime(), Integer.toString(j)));
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
            }

            if (serverRestart) {
                shutdownCorfuServer(corfuServerProcess);
                assert !corfuServerProcess.isAlive();
                runCorfuServer();
            }
        }

        shutdownCorfuServer(corfuServerProcess);
    }

    @Test
    public void testRecoveryWith3FailoversAnd3Keys() throws Exception {

        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime(), "A");

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);
        map.put("a", expectedValue + 1);
        map.put("a", expectedValue + 2);
        map.put("c", expectedValue + 1);

        shutdownCorfuServer(corfuServerProcess);
        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();
        map = createMap(createRuntime(), "A");
        assertThat(map.get("a")).isEqualTo(expectedValue + 2);
        assertThat(map.get("c")).isEqualTo(expectedValue + 1);

        final int expectedValue2 = 10;
        map.put("a", expectedValue2);
        map.put("b", expectedValue2);
        map.put("c", expectedValue2 + 1);

        shutdownCorfuServer(corfuServerProcess);
        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();
        map = createMap(createRuntime(), "A");
        assertThat(map.get("a")).isEqualTo(expectedValue2);
        assertThat(map.get("b")).isEqualTo(expectedValue2);
        assertThat(map.get("c")).isEqualTo(expectedValue2 + 1);
        map.put("a", expectedValue2 + 1);
        map.put("a", expectedValue2 + 2);
        map.put("b", expectedValue);

        shutdownCorfuServer(corfuServerProcess);
        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();
        map = createMap(createRuntime(), "A");
        assertThat(map.get("a")).isEqualTo(expectedValue2 + 2);
        assertThat(map.get("b")).isEqualTo(expectedValue);
        assertThat(map.get("c")).isEqualTo(expectedValue2 + 1);

        // final destroy
        shutdownCorfuServer(corfuServerProcess);
    }


}
