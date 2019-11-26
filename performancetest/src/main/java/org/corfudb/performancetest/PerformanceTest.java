package org.corfudb.performancetest;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.After;
import org.junit.Before;
import java.io.*;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * PerformanceTest is super class for different component performance test.
 * This class provide common fields and methods for all performance test classes.
 * @author Lin Dong
 */
@Slf4j
public class PerformanceTest {
    /**
     * Used for converting millisecond to second.
     */
    public static final int MILLI_TO_SECOND = 1000;
    /**
     * Used for loading properties in PerformanceTest.properties.
     */
    protected static final Properties PROPERTIES = new Properties();
    /**
     * Endpoint of corfuserver(default value is localhost:9000).
     */
    protected final String endPoint;
    /**
     * Port of corfuserver(default value is 9000)
     */
    protected final String port;
    /**
     * Random object used for generating random integer values based on a seed.
     */
    protected final Random random;
    /**
     * The directory where the collected metrics will be stored.
     * Default value: /Users/lidong/metrics
     */
    protected final String reportPath;
    /**
     * Runtime for all API calls to init the tests.
     */
    public CorfuRuntime runtime;
    /**
     * The report Interval of metrics CSV reporter.
     */
    protected final String reportInterval;
    /**
     * System property key string for report folder.
     */
    public static final String PROPERTY_CSV_FOLDER = "corfu.metrics.csv.folder";
    /**
     * System property key string for report interval.
     */
    public static final String PROPERTY_CSV_INTERVAL = "corfu.metrics.csv.interval";
    /**
     * System property key string controlling whether collect metrics.
     */
    public static final String PROPERTY_LOCAL_METRICS_COLLECTION =
            "corfu.local.metrics.collection";
    /**
     * MetricsFilter used for initiating CSVreporter.
     */
    private static final String ADDRESS_SPACE_METRIC_PREFIX = "corfu.runtime.as-view";
    private static final MetricFilter ADDRESS_SPACE_FILTER =
            (name, metric) -> !name.contains(ADDRESS_SPACE_METRIC_PREFIX);

    /**
     * Constructor of PerformanceTest. Load Properties from config file.
     * Init related variables.
     */
    public PerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            log.error(e.toString());
        }
        port = PROPERTIES.getProperty("port", "9000");
        endPoint = "localhost:" + port;
        reportPath = PROPERTIES.getProperty("reportPath", "/Users/lidong/metrics");
        reportInterval = PROPERTIES.getProperty("reportInterval", "10");
        random = new Random();
    }

    /**
     * Executed before each test. Make sure runtime and all servers are shutdown.
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        runtime = null;
        forceShutdownAllCorfuServers();
    }

    /**
     * Execute after each test. Shut down runtime and all servers.
     * @throws Exception
     */
    @After
    public void cleanUp() throws Exception {
        forceShutdownAllCorfuServers();
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    /**
     * Force shut down all servers.
     * @throws IOException
     * @throws InterruptedException
     */
    public void forceShutdownAllCorfuServers() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9");
        Process p = builder.start();
        p.waitFor();
    }

    /**
     * Set JVM flags to enable metrics report.
     * Start CSV reporter for each test.
     * @param testName testName used for creating different report directories for each test.
     */
    public void setMetricsReportFlags(String testName) {
        System.setProperty(PROPERTY_LOCAL_METRICS_COLLECTION, "true");
        System.setProperty(PROPERTY_CSV_INTERVAL, reportInterval);
        String testReportPath = reportPath + "/" + testName;
        File path = new File(testReportPath);
        if (!path.exists()) {
            path.mkdir();
        }
        System.setProperty(PROPERTY_CSV_FOLDER , testReportPath);
        CsvReporter csvReporter = CsvReporter.forRegistry(CorfuRuntime.getDefaultMetrics())
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .filter(ADDRESS_SPACE_FILTER)
                .build(path);
        csvReporter.start(1, TimeUnit.SECONDS);
    }

    /**
     * Init runtime.
     * @return new runtime.
     */
    public CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    /**
     * Start CorfuServer (default addresss: localhost:9000).
     * @return Process of the new CorfuServer.
     * @throws IOException
     */
    public Process runServer() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", "./scripts/run.sh");
        Process corfuServerProcess = builder.start();
        return corfuServerProcess;
    }

    /**
     * Get the pid of a Process.
     * @param p the Process.
     * @return pid.
     */
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

    /**
     * Kill a CorfuServer Process.
     * @param corfuServerProcess The Process need to be killed.
     * @throws IOException
     * @throws InterruptedException
     */
    public void killServer(Process corfuServerProcess) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        long pid = getPid(corfuServerProcess);
        builder.command("sh", "-c", "kill -" + pid);
        Process p = builder.start();
        p.waitFor();
    }
}
