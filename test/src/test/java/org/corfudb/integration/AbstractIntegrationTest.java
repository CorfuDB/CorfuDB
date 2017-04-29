package org.corfudb.integration;

import org.apache.commons.io.FileUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.After;
import org.junit.Before;

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

/**
 * Created by zlokhandwala on 4/28/17.
 */
public class AbstractIntegrationTest extends AbstractCorfuTest {

    private static final String TEST_HOST = "localhost";
    private static final String TEST_PORT = "9000";
    private static final String TEST_ENDPOINT = TEST_HOST + ":" + TEST_PORT;

    private static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;
    private static final String CORFU_SERVER_COMMAND = "bin/corfu_server -a " + TEST_HOST + " -sl " + CORFU_LOG_PATH + " -d TRACE " + TEST_PORT;
    private static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    private static final String CORFU_CONSOLELOG = CORFU_LOG_PATH + File.separator + "consolelog";
    private static final String KILL_COMMAND = "kill -9 ";
    private static final String KILL_CORFUSERVER = "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9";

    public AbstractIntegrationTest() {
        CorfuRuntime.overrideGetRouterFunction = null;
    }

    @Before
    public void setUp() throws Exception {
        forceShutdownAllCorfuServers();
        FileUtils.cleanDirectory(new File(CORFU_LOG_PATH));
    }

    @After
    public void cleanUp() throws Exception {
        forceShutdownAllCorfuServers();
    }

    /**
     * Runs the CorfuServer in a separate bash process.
     *
     * @return Corfu Process
     * @throws IOException
     * @throws InterruptedException
     */
    public static Process runCorfuServer() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", CORFU_SERVER_COMMAND);
        builder.directory(new File(CORFU_PROJECT_DIR));
        Process corfuServerProcess = builder.start();
        StreamGobbler streamGobbler = new StreamGobbler(corfuServerProcess.getInputStream(), CORFU_CONSOLELOG);
        Executors.newSingleThreadExecutor().submit(streamGobbler);
        return corfuServerProcess;
    }

    /**
     * Shuts down all corfu instances running on the node.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static void forceShutdownAllCorfuServers() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", KILL_CORFUSERVER);
        Process p = builder.start();
        p.waitFor();
    }

    public static boolean shutdownCorfuServer(Process corfuServerProcess) throws IOException, InterruptedException {
        long pid = getPid(corfuServerProcess);
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", KILL_COMMAND + pid);
        Process p = builder.start();
        p.waitFor();
        forceShutdownAllCorfuServers();
        return true;
    }

    public static long getPid(Process p) {
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

    public static CorfuRuntime createRuntime() {
        CorfuRuntime rt = new CorfuRuntime(TEST_ENDPOINT)
                .setCacheDisabled(true)
                .connect();
        return rt;

    }

    public static Map<String, Integer> createMap(CorfuRuntime rt, String streamName) {
        Map<String, Integer> map = rt.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setType(SMRMap.class)
                .open();
        return map;
    }

    public static class StreamGobbler implements Runnable {
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
