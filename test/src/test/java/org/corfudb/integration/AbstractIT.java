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
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Integration tests.
 * Created by zlokhandwala on 4/28/17.
 */
public class AbstractIT extends AbstractCorfuTest {

    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 9000;
    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;

    private static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;
    private static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    private static final String CORFU_CONSOLELOG = CORFU_LOG_PATH + File.separator + "consolelog";
    private static final String KILL_COMMAND = "pkill -9 -P ";
    private static final String FORCE_KILL_ALL_CORFU_COMMAND = "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9";

    private static final int SHUTDOWN_RETRIES = 10;
    private static final long SHUTDOWN_RETRY_WAIT = 500;

    static public Properties PROPERTIES;

    public static final String TEST_SEQUENCE_LOG_PATH = CORFU_LOG_PATH + File.separator + "testSequenceLog";

    public AbstractIT() {
        CorfuRuntime.overrideGetRouterFunction = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("CorfuDB.properties");
        PROPERTIES = new Properties();
        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cleans up the corfu log directory before running any test.
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        forceShutdownAllCorfuServers();
        FileUtils.cleanDirectory(new File(CORFU_LOG_PATH));
    }

    /**
     * Cleans up all Corfu instances after the tests.
     *
     * @throws Exception
     */
    @After
    public void cleanUp() throws Exception {
        forceShutdownAllCorfuServers();
    }

    /**
     * Runs the CorfuServer in a separate bash process.
     * By default the server starts on localhost:9000.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static Process runCorfuServer() throws IOException, InterruptedException {
        return runCorfuServer(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Runs the CorfuServer in a separate bash process.
     *
     * @param host
     * @param port
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static Process runCorfuServer(String host, int port) throws IOException, InterruptedException {
        File logPath = new File(getCorfuServerLogPath(host, port));
        if (!logPath.exists()) {
            logPath.mkdir();
        }

        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", getRunServerCommand(host, port, getCorfuServerLogPath(host, port)));
        builder.directory(new File(CORFU_PROJECT_DIR));
        Process corfuServerProcess = builder.start();
        StreamGobbler streamGobbler = new StreamGobbler(corfuServerProcess.getInputStream(), CORFU_CONSOLELOG);
        Executors.newSingleThreadExecutor().submit(streamGobbler);
        return corfuServerProcess;
    }

    public static String getCorfuServerLogPath(String host, int port) {
        return CORFU_LOG_PATH + File.separator + host + "_" + port + "_log";
    }

    private static String getRunServerCommand(String host, int port, String logPath) {
        return "bin/corfu_server -a " + host + " -sl " + logPath + " -d TRACE " + port;
    }


    /**
     * Shuts down all corfu instances running on the node.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static void forceShutdownAllCorfuServers() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", FORCE_KILL_ALL_CORFU_COMMAND);
        Process p = builder.start();
        p.waitFor();
    }

    /**
     * Shuts down all corfu instances.
     * TODO: Should be able to gracefully kill a single specified corfu server.
     *
     * @param corfuServerProcess
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean shutdownCorfuServer(Process corfuServerProcess) throws IOException, InterruptedException {
        int retries = SHUTDOWN_RETRIES;
        while (true) {
            long pid = getPid(corfuServerProcess);
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", KILL_COMMAND + pid);
            Process p = builder.start();
            p.waitFor();

            if (retries == 0) {
                return false;
            }
            if (corfuServerProcess.isAlive()) {
                retries--;
                Thread.sleep(SHUTDOWN_RETRY_WAIT);
            } else {
                return true;
            }
        }
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

    public static CorfuRuntime createDefaultRuntime() {
        return createRuntime(DEFAULT_ENDPOINT);
    }

    public static CorfuRuntime createRuntime(String endpoint) {
        CorfuRuntime rt = new CorfuRuntime(endpoint)
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
