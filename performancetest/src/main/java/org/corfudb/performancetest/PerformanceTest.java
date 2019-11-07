package org.corfudb.performancetest;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.After;
import org.junit.Before;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Lin Dong on 10/25/19.
 */

@Slf4j
public class PerformanceTest {
    protected static final Properties PROPERTIES = new Properties();
    protected final String endPoint;
    protected final String port;
    protected final int metricsPort;
    protected final Random random;
    protected final String reportPath;
    public CorfuRuntime runtime;
    protected final String reportInterval;
    public static final String PROPERTY_CSV_FOLDER = "corfu.metrics.csv.folder";
    public static final String PROPERTY_CSV_INTERVAL = "corfu.metrics.csv.interval";
    public static final String PROPERTY_LOCAL_METRICS_COLLECTION =
            "corfu.local.metrics.collection";
    private static final String ADDRESS_SPACE_METRIC_PREFIX = "corfu.runtime.as-view";
    private static final MetricFilter ADDRESS_SPACE_FILTER =
            (name, metric) -> !name.contains(ADDRESS_SPACE_METRIC_PREFIX);

    public PerformanceTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerformanceTest.properties");

        try {
            PROPERTIES.load(input);
        } catch (IOException e) {
            log.error(e.toString());
        }
        metricsPort = Integer.parseInt(PROPERTIES.getProperty("sequencerMetricsPort", "1000"));
        port = PROPERTIES.getProperty("port", "9000");
        endPoint = "localhost:" + port;
        reportPath = PROPERTIES.getProperty("reportPath", "/Users/lidong/metrics");
        reportInterval = PROPERTIES.getProperty("reportInterval", "10");
        random = new Random();
    }

    @Before
    public void setUp() throws Exception {
        runtime = null;
        forceShutdownAllCorfuServers();
    }

    @After
    public void cleanUp() throws Exception {
        forceShutdownAllCorfuServers();
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    public void forceShutdownAllCorfuServers() throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", "jps | grep CorfuServer|awk '{print $1}'| xargs kill -9");
        Process p = builder.start();
        p.waitFor();
    }

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

    public CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    public Process runServer() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", "./scripts/run.sh");
        Process corfuServerProcess = builder.start();
        return corfuServerProcess;
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

    public void killServer(Process corfuServerProcess) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder();
        long pid = getPid(corfuServerProcess);
        builder.command("sh", "-c", "kill -" + pid);
        Process p = builder.start();
        p.waitFor();
    }
}
