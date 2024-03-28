package org.corfudb.integration;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ISerializer;
import org.junit.After;
import org.junit.Before;
import org.mockito.MockitoAnnotations;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class contains basic functionality for any IT test.
 * Any IT test should inherit this test.
 */
@Slf4j
public class AbstractIT extends AbstractCorfuTest {

    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 9000;
    static final int DEFAULT_HEALTH_PORT = 8080;
    static final int DEFAULT_LOG_REPLICATION_PORT = 9020;

    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;

    static final String CORFU_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    public static final String CORFU_LOG_PATH = PARAMETERS.TEST_TEMP_DIR;

    static final long DEFAULT_MVO_CACHE_SIZE = 100;

    private static final String KILL_COMMAND = "pkill -9 -P ";
    // FIXME: if jps doesn't exist tear down will fail silently
    private static final String FORCE_KILL_ALL_CORFU_COMMAND = "jps | grep -e CorfuServer -e CorfuInterClusterReplicationServer|awk '{print $1}'| xargs kill -9";
    private static final String GRACEFULLY_KILL_ALL_CORFU_COMMAND = "jps | grep -e CorfuServer -e CorfuInterClusterReplicationServer|awk '{print $1}'| xargs kill -SIGTERM";
    public boolean shouldForceKill = false;

    // Bash command literals to avoid string duplicate usage errors from Static Code Analysis
    private static final String SH = "sh";
    private static final String HYPHEN_C = "-c";
    private static final int SHUTDOWN_RETRIES = 10;
    private static final long SHUTDOWN_RETRY_WAIT = 500;

    // Config the msg size for log replication data
    // sent from active cluster to the standby cluster.
    // We set it as 128KB to make multiple messages during the tests.
    private static final int MSG_SIZE = 131072;

    private final ConcurrentLinkedQueue<CorfuRuntime> runtimes = new ConcurrentLinkedQueue<>();

    public static final Properties PROPERTIES = new Properties();

    public static final String TEST_SEQUENCE_LOG_PATH = CORFU_LOG_PATH + File.separator + "testSequenceLog";


    public AbstractIT() {
        MockitoAnnotations.openMocks(this);
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
     * @throws Exception error
     */
    @Before
    public void setUp() throws Exception {
        // These tests kill the Corfu or LR Server in the middle. The shutdown must happen
        // immediately so that the test can succeed. The coverage data from these servers is
        // not collected due to force kill. However, during normal shutdown, we need to gracefully
        // shut them down for jacoco agent to collect coverage data.
        String[] classNames = {"CorfuReplicationTrimIT",
                "CorfuReplicationUpgradeIT",
                "CorfuReplicationReconfigurationIT",
                "CorfuReplicationClusterConfigIT",
                "WorkflowIT",
                "ClusterReconfigIT"};
        if (Arrays.asList(classNames).contains(this.getClass().getSimpleName())) {
            shouldForceKill = true;
        }

        shutdownAllCorfuServers(shouldForceKill);
        FileUtils.cleanDirectory(new File(CORFU_LOG_PATH));
    }

    public static String getCodeCoverageCmd() {
        // Check if coverage is enabled and signal the shell script by setting an env variable
        // (for enabling pass -Dcode-coverage=true to the maven command)
        if (System.getProperty("code-coverage")!= null &&
                System.getProperty("code-coverage").equals("true")) {
            // log.info("System.getProperty(\"code-coverage\")={}",System.getProperty("code-coverage") );
            return "export CODE_COVERAGE=true;";
        } else {
            return "";
        }
    }

    private static String getMetricsCmd(String metricsConfigFile) throws IOException {
        if (!metricsConfigFile.isEmpty()) {
            URL resource = AbstractIT.class.getResource("/" + metricsConfigFile);
            try {
                String configPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
                return "export METRICS_CONFIG_FILE=" + configPath + ";";
            } catch (URISyntaxException use) {
                throw new IOException(use);
            }
        } else {
            return "";
        }
    }

    /**
     * Cleans up all Corfu instances after the tests.
     *
     * @throws Exception error
     */
    @After
    public void cleanUp() throws Exception {
        shutdownAllCorfuServers(shouldForceKill);

        while (!runtimes.isEmpty()){
            CorfuRuntime rt = runtimes.poll();
            try {
                rt.shutdown();
            } catch (Throwable th){
                log.warn("Error closing a runtime", th);
            }
        }
    }

    public static String getCorfuServerLogPath(String host, int port) {
        return CORFU_LOG_PATH + File.separator + host + "_" + port + "_log";
    }

    /**
     * Shuts down all corfu instances running on the node.
     *
     * @throws Exception error
     */
    public static void shutdownAllCorfuServers(boolean shouldForceKill) throws Exception {
        ProcessBuilder builder = new ProcessBuilder();
        if (shouldForceKill){
            builder.command(SH, HYPHEN_C, FORCE_KILL_ALL_CORFU_COMMAND);
        } else {
            builder.command(SH, HYPHEN_C, GRACEFULLY_KILL_ALL_CORFU_COMMAND);
        }
        Process p = builder.start();
        p.waitFor();
    }

    /**
     * Shuts down all corfu instances.
     *
     * @param corfuServerProcess corfu server
     * @return operation result
     * @throws Exception error
     */
    public static boolean shutdownCorfuServer(Process corfuServerProcess) throws Exception {
        int retries = SHUTDOWN_RETRIES;
        while (true) {
            Optional<Long> processPid = getPid(corfuServerProcess);
            if (processPid.isPresent()) {
                long parentPid = processPid.get();
                // Get Children PIDs
                List<Long> pidList = getChildPIDs(parentPid);
                pidList.add(parentPid);

                ProcessBuilder builder = new ProcessBuilder();
                for (Long pid : pidList) {
                    builder.command(SH, HYPHEN_C, KILL_COMMAND + pid);
                    Process p = builder.start();
                    p.waitFor();
                }
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

    public void restartServer(CorfuRuntime corfuRuntime, String endpoint) throws InterruptedException {
        corfuRuntime.invalidateLayout();
        RuntimeLayout runtimeLayout = corfuRuntime.getLayoutView().getRuntimeLayout();
        try {
            runtimeLayout.getBaseClient(endpoint).restart().get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error", e);
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
    public static Layout waitForLayoutChange(
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

        return refreshedLayout;
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
     */
    private static List<Long> getChildPIDs(long pid) {
        List<Long> childPIDs = new ArrayList<>();
        try {
            // Get child pid(s)
            ProcessBuilder builder = new ProcessBuilder();
            builder.command(SH, HYPHEN_C, "pgrep -P " + pid);
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
                List<Long> pidRecursive = getChildPIDs(childPID);
                childPIDs.addAll(pidRecursive);
            }

        } finally {
            return childPIDs;
        }
    }

    public static Optional<Long> getPid(Process p) {
        Optional<Long> pid = Optional.empty();
        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")
                    || p.getClass().getName().equals("java.lang.ProcessImpl")) {
                pid = Optional.of(p.pid());
            }
        } catch (Exception e) {
            log.info("Unable to get Process ID", e);
        }
        return pid;
    }

    /**
     * Creates a message of specified size in bytes.
     *
     * @param msgSize message size
     * @return a string with a message size
     */
    public static String createStringOfSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    public CorfuRuntime createDefaultRuntime() {
        CorfuRuntime rt = createRuntime(DEFAULT_ENDPOINT);
        managed(rt);
        return rt;
    }

    /**
     * Make corfu runtime managed and shut it down after a test finishes
     * @param runtime corfu runtime
     */
    void managed(CorfuRuntime runtime) {
        runtimes.add(runtime);
    }

    public Process runServer(int port, boolean single) throws IOException {
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setSingle(single)
                .runServer();
    }

    public Process runReplicationServer(int port) throws IOException {
        return new CorfuReplicationServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .runServer();
    }

    public Process runReplicationServer(int port, String pluginConfigFilePath) throws IOException {
        return new CorfuReplicationServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setPluginConfigFilePath(pluginConfigFilePath)
                .setMsg_size(MSG_SIZE)
                .runServer();
    }

    public Process runReplicationServer(int port, String pluginConfigFilePath, int lockLeaseDuration) throws IOException {
        return new CorfuReplicationServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setLockLeaseDuration(lockLeaseDuration)
                .setPluginConfigFilePath(pluginConfigFilePath)
                .setMsg_size(MSG_SIZE)
                .runServer();
    }

    public Process runReplicationServerCustomMaxWriteSize(int port,
                                                          String pluginConfigFilePath, int maxWriteSize,
                                                          int maxEntriesApplied) throws IOException {
        return new CorfuReplicationServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(port)
                .setPluginConfigFilePath(pluginConfigFilePath)
                .setMsg_size(MSG_SIZE)
                .setMaxWriteSize(maxWriteSize)
                .setMaxSnapshotEntriesApplied(maxEntriesApplied)
                .runServer();
    }

    public Process runDefaultServer() throws IOException {
        return new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .setLogPath(getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .runServer();
    }

    public Process runPersistentServer(String address, int port, boolean singleNode) throws IOException {
        return new CorfuServerRunner()
                .setHost(address)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(address, port))
                .setSingle(singleNode)
                .runServer();
    }

    public CorfuRuntime createRuntime(String endpoint) {
        return createRuntime(endpoint, true, CorfuRuntime.CorfuRuntimeParameters.builder());
    }

    public CorfuRuntime createRuntime(String endpoint, CorfuRuntimeParametersBuilder parametersBuilder) {
        return createRuntime(endpoint, true, parametersBuilder);
    }

    public CorfuRuntime createRuntimeWithCache() {
        return createRuntimeWithCache(DEFAULT_ENDPOINT);
    }

    public CorfuRuntime createRuntimeWithCache(String endpoint) {
        return createRuntime(endpoint, false, CorfuRuntime.CorfuRuntimeParameters.builder());
    }

    private CorfuRuntime createRuntime(String endpoint, boolean cacheDisabled,
                                              CorfuRuntimeParametersBuilder parametersBuilder) {
        CorfuRuntimeParameters rtParams = parametersBuilder
                .maxMvoCacheEntries(DEFAULT_MVO_CACHE_SIZE)
                .cacheDisabled(cacheDisabled)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(rtParams)
                .parseConfigurationString(endpoint)
                .connect();

        managed(rt);

        return rt;
    }

    public static <K, V> PersistentCorfuTable<K, V> createCorfuTable(@NonNull CorfuRuntime rt,
                                                                     @NonNull String streamName) {
        return rt.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(PersistentCorfuTable.<K, V>getTypeToken())
                .open();
    }

    public static <K, V> PersistentCorfuTable<K, V> createCorfuTable(@NonNull CorfuRuntime rt,
                                                                     @NonNull String streamName,
                                                                     @NonNull ISerializer serializer) {
        // Serializer should be registered with the runtime separately
        return rt.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setSerializer(serializer)
                .setTypeToken(PersistentCorfuTable.<K, V>getTypeToken())
                .open();
    }

    public Token checkpointAndTrim(
            CorfuRuntime runtime, String namespace, List<String> tablesToCheckpoint, boolean partialTrim) {

        MultiCheckpointWriter<PersistentCorfuTable<?, ?>> mcw = new MultiCheckpointWriter<>();
        tablesToCheckpoint.forEach(tableName -> {
            String fqTableName = TableRegistry.getFullyQualifiedTableName(namespace, tableName);
            mcw.addMap(
                    createCorfuTable(runtime, fqTableName)
            );
        });

        CorfuRuntime rt = createRuntime(runtime.getLayoutServers().get(0));

        // Add Registry Table
        mcw.addMap(rt.getTableRegistry().getRegistryTable());
        mcw.addMap(rt.getTableRegistry().getProtobufDescriptorTable());
        // Checkpoint & Trim
        Token trimPoint = mcw.appendCheckpoints(rt, "StreamingIT");
        if (partialTrim) {
            final int trimOffset = 5;
            long sequenceModified = trimPoint.getSequence() - trimOffset;
            Token partialTrimMark = Token.of(trimPoint.getEpoch(), sequenceModified);
            rt.getAddressSpaceView().prefixTrim(partialTrimMark);
        } else {
            rt.getAddressSpaceView().prefixTrim(trimPoint);
        }
        rt.getAddressSpaceView().gc();
        rt.getObjectsView().getObjectCache().clear();
        return trimPoint;
    }

    public static class StreamGobbler implements Runnable {
        private final InputStream inputStream;
        private final String logfile;

        public StreamGobbler(InputStream inputStream, String logfile) throws IOException {
            this.inputStream = inputStream;
            this.logfile = logfile;
            Path path = Paths.get(logfile);
            if (Files.notExists(path)) {
                Files.createFile(path);
            }
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Consumer<String> stringWriterTask = data -> {
                try {
                    Path path = Paths.get(logfile);
                    Files.write(path, data.getBytes(), StandardOpenOption.APPEND);
                    Files.write(path, "\n".getBytes(), StandardOpenOption.APPEND);
                } catch (Exception e) {
                    log.error("StreamGobbler: Error", e);
                }
            };
            reader.lines().forEach(stringWriterTask);
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
        private int healthPort = -1;
        private String metricsConfigFile = "";
        private boolean single = true;
        private boolean tlsEnabled = false;
        private boolean tlsMutualAuthEnabled = false;
        private boolean noAutoCommit = true;
        private String keyStore = null;
        private String keyStorePassword = null;
        private String logLevel = "INFO";
        private String logPath = null;
        private String trustStore = null;
        private String logSizeLimitPercentage = null;
        private String trustStorePassword = null;
        private String disableCertExpiryCheckFile = null;
        private String compressionCodec = null;
        private boolean disableHost = false;
        private String networkInterface = null;
        private NetworkInterfaceVersion networkInterfaceVersion = null;
        private boolean disableLogUnitServerCache = false;


        /**
         * Create a command line string according to the properties set for a Corfu Server
         * Instance
         *
         * @return command line including options that captures the properties of Corfu Server instance
         */
        public String getOptionsString() {
            StringBuilder command = new StringBuilder();

            if (disableLogUnitServerCache) {
                command.append("-c ").append(0);
            }

            if (!disableHost) {
                command.append(" -a ").append(host);
            }

            if (logPath != null) {
                command.append(" -l ").append(logPath);
            } else {
                command.append(" -m");
            }

            if (!metricsConfigFile.isEmpty()) {
                command.append(" --metrics ");
            }

            if (healthPort != -1) {
                command.append(" --health-port=").append(healthPort);
            }

            if (single) {
                command.append(" -s");
            }

            if (noAutoCommit) {
                command.append(" -A");
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
                if (tlsMutualAuthEnabled) {
                    command.append(" -b");
                }
                if (disableCertExpiryCheckFile != null) {
                    command.append(" --disable-cert-expiry-check-file=").append(disableCertExpiryCheckFile);
                }
            }

            if (networkInterface != null) {
                command.append(" --network-interface=").append(networkInterface);
            }

            if (networkInterfaceVersion != null) {
                command.append(" --network-interface-version=").append(networkInterfaceVersion);
            }

            command.append(" -d ").append(logLevel).append(" ")
                    .append(port);

            return command.toString();
        }

        /**
         * Creates a server with the options set according to the properties of this Corfu server instance
         *
         * @return a {@link Process} running a Corfu server as it is setup through the properties of
         * the instance on which this method is called.
         * @throws IOException error
         */
        public Process runServer() throws IOException {
            final String serverConsoleLogPath = CORFU_LOG_PATH + File.separator + host + "_" + port + "_consolelog";
            File logPath = new File(getCorfuServerLogPath(host, port));
            if (!logPath.exists()) {
                logPath.mkdir();
            }
            ProcessBuilder builder = new ProcessBuilder();

            builder.command(SH, HYPHEN_C, getCodeCoverageCmd() + getMetricsCmd(metricsConfigFile) +
                    " bin/corfu_server " + getOptionsString());
            builder.directory(new File(CORFU_PROJECT_DIR));
            Process corfuServerProcess = builder.redirectErrorStream(true).start();
            StreamGobbler streamGobbler = new StreamGobbler(corfuServerProcess.getInputStream(), serverConsoleLogPath);
            Executors.newSingleThreadExecutor().submit(streamGobbler);
            return corfuServerProcess;
        }
    }

    /**
     * This is a helper class for setting up the properties of a CorfuLogReplicationServer and
     * creating an instance of a Corfu Log Replication Server accordingly.
     */
    @Getter
    @Setter
    @Accessors(chain = true)
    public static class CorfuReplicationServerRunner {

        private String host = DEFAULT_HOST;
        private int port = DEFAULT_LOG_REPLICATION_PORT;
        private String metricsConfigFile = "";
        private boolean tlsEnabled = false;
        private boolean tlsMutualAuthEnabled = false;
        private String keyStore = null;
        private String keyStorePassword = null;
        private String logLevel = "INFO";
        private String trustStore = null;
        private String trustStorePassword = null;
        private String disableCertExpiryCheckFile = null;
        private String compressionCodec = null;
        private String pluginConfigFilePath = null;
        private String logPath = null;
        private int msg_size = 0;
        private Integer lockLeaseDuration;
        private int maxWriteSize = 0;
        private int maxSnapshotEntriesApplied;

        /**
         * Create a command line string according to the properties set for a Corfu Server
         * Instance
         *
         * @return command line including options that captures the properties of Corfu Server instance
         */
        public String getOptionsString() {
            StringBuilder command = new StringBuilder();
            command.append("-a ").append(host);

            if (msg_size != 0) {
                command.append(" --max-replication-data-message-size=").append(msg_size);
            }

            if (logPath != null) {
                command.append(" -l ").append(logPath);
            } else {
                command.append(" -m");
            }
            if (!metricsConfigFile.isEmpty()) {
                command.append(" --metrics ");
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
                if (tlsMutualAuthEnabled) {
                    command.append(" -b");
                }
                if (disableCertExpiryCheckFile != null) {
                    command.append(" --disable-cert-expiry-check-file=").append(disableCertExpiryCheckFile);
                }
            }

            if (pluginConfigFilePath != null) {
                command.append(" --plugin=").append(pluginConfigFilePath);
            }

            if (lockLeaseDuration != null) {
                command.append(" --lock-lease=").append(lockLeaseDuration);
            }

            if (maxWriteSize != 0) {
                command.append(" --max-replication-write-size=").append(maxWriteSize);
            }

            if (maxSnapshotEntriesApplied != 0) {
                command.append(" --max-snapshot-entries-applied=").append(maxSnapshotEntriesApplied);
            }

            command.append(" -d ").append(logLevel).append(" ")
                    .append(port);

            return command.toString();
        }

        /**
         * Creates a server with the options set according to the properties of this Corfu server instance
         *
         * @return a {@link Process} running a Corfu server as it is setup through the properties of
         * the instance on which this method is called.
         * @throws IOException error
         */
        public Process runServer() throws IOException {
            final String serverConsoleLogPath = CORFU_LOG_PATH + File.separator + host + "_" + port + "_consolelog";
            File logPath = new File(getCorfuServerLogPath(host, port));
            if (!logPath.exists()) {
                logPath.mkdir();
            }
            ProcessBuilder builder = new ProcessBuilder();

            builder.command(SH, HYPHEN_C, getCodeCoverageCmd() + getMetricsCmd(metricsConfigFile) +
                    " bin/corfu_replication_server " + getOptionsString());

            builder.directory(new File(CORFU_PROJECT_DIR));
            Process corfuReplicationServerProcess = builder.redirectErrorStream(true).start();
            StreamGobbler streamGobbler = new StreamGobbler(corfuReplicationServerProcess.getInputStream(), serverConsoleLogPath);
            Executors.newSingleThreadExecutor().submit(streamGobbler);
            return corfuReplicationServerProcess;
        }
    }
}
