package org.corfudb.performancetest;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

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
    protected final String reportInterval;

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

    protected void setMetricsReportFlags(String testName) {
        System.setProperty("corfu.local.metrics.collection", "true");
        System.setProperty("corfu.metrics.csv.interval", reportInterval);
        String testReportPath = reportPath + testName;
        File logPath = new File(testReportPath);
        if (!logPath.exists()) {
            logPath.mkdir();
        }
        System.setProperty("corfu.metrics.csv.folder", reportPath);
    }

    protected CorfuRuntime initRuntime() {
        CorfuRuntime.CorfuRuntimeParameters parameters = CorfuRuntime.CorfuRuntimeParameters.builder().build();
        parameters.setPrometheusMetricsPort(metricsPort);
        CorfuRuntime corfuRuntime = CorfuRuntime.fromParameters(parameters);
        corfuRuntime.addLayoutServer(endPoint);
        corfuRuntime.connect();
        return corfuRuntime;
    }

    protected Process runServer() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command("./scripts/run.sh");
        Process corfuServerProcess = builder.start();
        return corfuServerProcess;
    }

    protected void killServer() throws IOException, InterruptedException {
        ProcessBuilder kill = new ProcessBuilder();
        kill.command("sh", "-c", "kill $(lsof -t -i:" + port + ")");
        Process killP = kill.start();
        killP.waitFor();
    }
}
