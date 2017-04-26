package org.corfudb.integration;

import org.apache.commons.io.FileUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.NetworkException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the recovery of the sequencer.
 * Created by zlokhandwala on 4/25/17.
 */
public class ServerRestartTest extends AbstractCorfuTest {

    private static final String TEST_HOST = "localhost";
    private static final String TEST_PORT = "9000";
    private static final String TEST_ENDPOINT = TEST_HOST + ":" + TEST_PORT;

    private static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;
    private static final String CORFU_SERVER_COMMAND = "bin/corfu_server -a " + TEST_HOST + " -sl " + CORFU_LOG_PATH + " -d TRACE " + TEST_PORT;
    private static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    private static final String CORFU_CONSOLELOG = CORFU_LOG_PATH + File.separator + "consolelog";
    private static final String KILL_COMMAND = "kill -9 ";
    private static final String KILL_CORFUSERVER = "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9";

    public ServerRestartTest() {
        CorfuRuntime.overrideGetRouterFunction = null;
    }

    public static void setUp() throws Exception {
        FileUtils.cleanDirectory(new File(CORFU_LOG_PATH));
    }

    private static Process runCorfuServer() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", CORFU_SERVER_COMMAND);
        builder.directory(new File(CORFU_PROJECT_DIR));
        Process corfuServerProcess = builder.start();
        StreamGobbler streamGobbler = new StreamGobbler(corfuServerProcess.getInputStream(), CORFU_CONSOLELOG);
        Executors.newSingleThreadExecutor().submit(streamGobbler);
        return corfuServerProcess;
    }

    private static boolean shutdownCorfuServer(Process corfuServerProcess) throws IOException, InterruptedException {
        long pid = getPid(corfuServerProcess);
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", KILL_COMMAND + pid);
        Process p = builder.start();
        p.waitFor();
        builder = new ProcessBuilder();
        builder.command("sh", "-c", KILL_CORFUSERVER);
        p = builder.start();
        p.waitFor();
        return true;
    }

    private static long getPid(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }

    private static CorfuRuntime createRuntime() {
        CorfuRuntime rt = new CorfuRuntime(TEST_ENDPOINT)
                .connect();
        return rt;

    }

    private static Map<String, Integer> createMap(CorfuRuntime rt) {
        Map<String, Integer> map = rt.getObjectsView()
                .build()
                .setStreamName("A")
                .setType(SMRMap.class)
                .open();
        return map;
    }

    /**
     * Test recovery with a failed over server and a new client.
     *
     * @throws Exception
     */
    @Test
    public void testSequencerRecoveryFailoverServerAndClient() throws Exception {
        setUp();
        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime());

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);

        shutdownCorfuServer(corfuServerProcess);

        assert !corfuServerProcess.isAlive();

        corfuServerProcess = runCorfuServer();

        map = createMap(createRuntime());

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
    public void testSequencerRecoveryFailoverClient() throws Exception {
        setUp();
        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime());

        final int expectedValue = 5;

        Integer previous = map.get("a");
        assertThat(previous).isNull();
        map.put("a", expectedValue);

        map = createMap(createRuntime());

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
    public void testSequencerRecoveryFailoverServer() throws Exception {
        setUp();
        Process corfuServerProcess = runCorfuServer();
        Map<String, Integer> map = createMap(createRuntime());

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

    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private String logfile;

        public StreamGobbler(InputStream inputStream, String logfile) {
            this.inputStream = inputStream;
            this.logfile = logfile;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach((x) -> {
                                try {
                                    Files.write(Paths.get(logfile), x.getBytes());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                    );
        }
    }


}
