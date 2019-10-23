package org.corfudb.integration.performance;

import com.codahale.metrics.MetricRegistry;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.CorfuRuntime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class AbstractPerfIT extends AbstractIT {
    static final String METRIC_PREFIX = "corfu-perf-";
    static final int RANDOM_UPPER_BOUND = 100;
    static final Properties PERF_PROPERTIES = new Properties();
    static MetricRegistry metricRegistry;

    AbstractPerfIT() {
        metricRegistry = CorfuRuntime.getDefaultMetrics();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("Performance.properties");

        try {
            PERF_PROPERTIES.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // setup metrics report
    }
}
