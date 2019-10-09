package org.corfudb.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.Sleep;
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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Integration tests.
 * Created by zlokhandwala on 4/28/17.
 */
@Slf4j
public class AbstractIT extends AbstractCorfuTest {
    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 9000;
    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;

    static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;

    private static final String KILL_COMMAND = "pkill -9 -P ";
    private static final String FORCE_KILL_ALL_CORFU_COMMAND = "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9";

    private static final int SHUTDOWN_RETRIES = 10;
    private static final long SHUTDOWN_RETRY_WAIT = 500;

    public CorfuRuntime runtime;

    public static final Properties PROPERTIES = new Properties();

    public static final String TEST_SEQUENCE_LOG_PATH = CORFU_LOG_PATH + File.separator + "testSequenceLog";


    public AbstractIT() {
        CorfuRuntime.overrideGetRouterFunction = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("CorfuDB.properties");

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
        runtime = null;
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
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    public static String getCorfuServerLogPath(String host, int port) {
        return CORFU_LOG_PATH + File.separator + host + "_" + port + "_log";
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
     *
     * @param corfuServerProcess
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static boolean shutdownCorfuServer(Process corfuServerProcess) throws IOException, InterruptedException {
        int retries = SHUTDOWN_RETRIES;
        while (true) {
            long parentPid = getPid(corfuServerProcess);
            // Get Children PIDs
            List<Long> pidList = getChildPIDs(parentPid);
            pidList.add(parentPid);

            ProcessBuilder builder = new ProcessBuilder();
            for (Long pid : pidList) {
                builder.command("sh", "-c", KILL_COMMAND + pid.longValue());
                Process p = builder.start();
                p.waitFor();
             }

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

    public void restartServer(
            CorfuRuntime corfuRuntime, String endpoint) throws InterruptedException {

        corfuRuntime.invalidateLayout();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout();
        try {
            runtimeLayout.getBaseClient(endpoint).restart().get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error: {}", e);
        }

        // The shutdown and restart can take an unknown amount of time and there is a chance that
        // the newer runtime may also connect to the older corfu server (before restart).
        // Hence the while loop.
        while (true) {
            if (corfuRuntime.getLayoutView().getLayout().getEpoch()
                    == (runtimeLayout.getLayout().getEpoch() + 1)) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            corfuRuntime.invalidateLayout();
        }
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(
            Predicate<Layout> verifier, CorfuRuntime corfuRuntime) throws InterruptedException {

        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    /**
     * Wait for the Supplier (some condition) to return true.
     *
     * @param supplier Supplier to test condition
     */
    public static void waitFor(Supplier<Boolean> supplier) throws InterruptedException {
        while (!supplier.get()) {
            TimeUnit.MILLISECONDS.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
    }

    /**
     * Get list of children (descendant) process identifiers (recursive)
     *
     * @param pid parent process identifier
     * @return list of children process identifiers
     *
     * @throws IOException
     */
    private static List<Long> getChildPIDs (long pid) {
        List<Long> childPIDs = new ArrayList<>();
        try {
            // Get child pid(s)
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", "pgrep -P " + pid);
            Process p = builder.start();
            p.waitFor();

            // Read output
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
            String previous = null;
            while ((line = br.readLine()) != null) {
                if (!line.equals(previous)) {
                    previous = line;
                    long childPID = Long.parseLong(line);
                    childPIDs.add(childPID);
                }
            }

            // Recursive lookup of children pids
            for (Long childPID : childPIDs) {
                List<Long> pidRecursive = getChildPIDs(childPID.longValue());
                childPIDs.addAll(pidRecursive);
            }

        } catch (IOException e) {
            throw e;
        } finally {
            return childPIDs;
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

    public static Process runServer(int port, boolean single) throws IOException {
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setSingle(single)
                .runServer();
    }

    public static Process runDefaultServer() throws IOException {
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .setLogPath(getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .runServer();
    }

    public static CorfuRuntime createRuntime(String endpoint) {
        CorfuRuntime rt = new CorfuRuntime(endpoint)
                .setCacheDisabled(true)
                .connect();
        return rt;
    }

    public static CorfuRuntime createRuntimeWithCache() {
        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT)
                .setCacheDisabled(false)
                .connect();
        return rt;
    }

    public static Map<String, Integer> createMap(CorfuRuntime rt, String streamName) {
        Map<String, Integer> map = rt.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                .open();
        return map;
    }

    public static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private String logfile;

        public StreamGobbler(InputStream inputStream, String logfile) throws IOException {
            this.inputStream = inputStream;
            this.logfile = logfile;
            if (Files.notExists(Paths.get(logfile))) {
                Files.createFile(Paths.get(logfile));
            }
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach((x) -> {
                                try {
                                    Files.write(Paths.get(logfile), x.getBytes(),
                                            StandardOpenOption.APPEND);
                                    Files.write(Paths.get(logfile), "\n".getBytes(),
                                            StandardOpenOption.APPEND);
                                } catch (Exception e) {
                                    log.error("StreamGobbler: Error, {}", e);
                                }
                            }
                    );
        }

    }

    /**
     * This is a helper class for setting up the properties of a CorfuServer and
     * creating an instance of a Corfu Server accordingly.
     */
    @Getter
    @Setter
    @Accessors(chain = true)
    public static class CorfuServerRunner {

        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;

        private boolean single = true;
        private boolean tlsEnabled = false;
        private String keyStore = null;
        private String keyStorePassword = null;
        private String logLevel = "INFO";
        private String logPath = null;
        private String trustStore = null;
        private String logSizeLimitPercentage = null;
        private String trustStorePassword = null;


        /**
         * Create a command line string according to the properties set for a Corfu Server
         * Instance
         * @return command line including options that captures the properties of Corfu Server instance
         */
        public String getOptionsString() {
            StringBuilder command = new StringBuilder();
            command.append("-a ").append(host);
            if (logPath != null) {
                command.append(" -l ").append(logPath);
            } else {
                command.append(" -m");
            }
            if (single) {
                command.append(" -s");
            }

            if (logSizeLimitPercentage != null) {
                command.append(" --log-size-quota-percentage ").append(logSizeLimitPercentage);
            }

            if (tlsEnabled) {
                command.append(" -e");
                if (keyStore != null) {
                    command.append(" -u ").append(keyStore);
                }
                if (keyStorePassword != null) {
                    command.append(" -f ").append(keyStorePassword);
                }
                if (trustStore != null) {
                    command.append(" -r ").append(trustStore);
                }
                if (trustStorePassword != null) {
                    command.append(" -w ").append(trustStorePassword);
                }
            }
            command.append(" -d ").append(logLevel).append(" ")
                    .append(port);
            return command.toString();
        }

        /**
         * Creates a server with the options set according to the properties of this Corfu server instance
         *
         * @return a {@link Process} running a Corfu server as it is setup through the properties of
         *         the instance on which this method is called.
         * @throws IOException
         */
        public Process runServer() throws IOException {
            final String serverConsoleLogPath = CORFU_LOG_PATH + File.separator + host + "_" + port + "_consolelog";

            File logPath = new File(getCorfuServerLogPath(host, port));
            if (!logPath.exists()) {
                logPath.mkdir();
            }
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", "bin/corfu_server " + getOptionsString());
            builder.directory(new File(CORFU_PROJECT_DIR));
            Process corfuServerProcess = builder.start();
            StreamGobbler streamGobbler = new StreamGobbler(corfuServerProcess.getInputStream(), serverConsoleLogPath);
            Executors.newSingleThreadExecutor().submit(streamGobbler);
            return corfuServerProcess;
        }
    }
}
