package org.corfudb.perf;

import com.codahale.metrics.MetricRegistry;
import org.corfudb.runtime.CorfuRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class AbstractPerfTest {
    public static final String METRIC_PREFIX = "corfu-perf-";
    public static final String CORFU_CLUSTER_HOST = "corfuClusterHost";
    public static final String CORFU_CLUSTER_PORT = "corfuClusterPort";
    public static final String RANDOM_SEED = "randomSeed";
    public static final int RANDOM_UPPER_BOUND = 100;


    final Properties properties = new Properties();
    static final MetricRegistry metricRegistry = new MetricRegistry();


    AbstractPerfTest() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("PerfTest.properties");

        try {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
