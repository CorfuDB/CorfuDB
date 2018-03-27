package org.corfudb.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@Slf4j
public class MetricsUtils {
    private static final FileDescriptorRatioGauge metricsJVMFdGauge =
            new FileDescriptorRatioGauge();
    private static final MetricSet metricsJVMGC = new GarbageCollectorMetricSet();
    private static final MetricSet metricsJVMMem = new MemoryUsageGaugeSet();
    private static final MetricSet metricsJVMThread = new ThreadStatesGaugeSet();

    private static int interval;
    @Getter
    private static boolean metricsCollectionEnabled = false;
    private static boolean metricsReportingEnabled = false;
    private static String mpTrigger = "filter-trigger"; // internal use only

    /**
     * Load metrics properties.
     *
     * <p>The expected properties in this properties file are:
     * <ul>
     * <li> interval: Integer taken from vm metricsInterval property
     * for the reporting interval for CSV output
     * </ul>
     *
     * <p>This function will be called to set the value of interval for
     * reporting. The value of interval is used as an indicator for setting
     * the metrics behavior. Positive values enable collection and reporting at
     * provided interval in seconds. Zero indicated only metrics collection.
     * Negative values disable both collection and reporting.
     */
    private static void loadVmProperties() {
        try {
            interval = Integer.valueOf(System.getProperty("metricsInterval"));
        } catch (NumberFormatException e) {
            log.error("Extracting metricsInterval VM property failed. " +
                    "Metrics collection and reporting will be disabled");
            interval = -1;
        }

        if (interval > 0) {
            metricsCollectionEnabled = true;
            metricsReportingEnabled = true;
        } else if (interval == 0) {
            metricsCollectionEnabled = true;
            metricsReportingEnabled = false;
        } else {
            metricsCollectionEnabled= false;
            metricsReportingEnabled = false;
        }
    }

    /**
     * Check if the metricsReportingSetup() function has been called
     * on 'metrics' before now.
     *
     * @param metrics Metric Registry
     * @return True if metricsReportingSetup() function has been called earlier
     */
    public static boolean isMetricsReportingSetUp(MetricRegistry metrics) {
        return metrics.getNames().contains(mpTrigger);
    }

    /**
     * Start metrics reporting via the Dropwizard 'CsvReporter' file writer.
     * Reporting can be turned on and off via the properties file described
     * in loadVmProperties()'s docs.  The report interval and report
     * directory cannot be altered at runtime.
     *
     * @param metrics Metrics registry
     */
    public static void metricsReportingSetup(MetricRegistry metrics) {
        metrics.counter(mpTrigger);
        loadVmProperties();

        if (metricsReportingEnabled) {
            MetricFilter f = (name, metric) -> {
                if (name.equals(mpTrigger)) {
                    loadVmProperties();
                    return false;
                }
                return metricsReportingEnabled;
            };

            Slf4jReporter reporter1 = Slf4jReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .outputTo(LoggerFactory.getLogger("org.corfudb.metricsdata"))
                    .filter(f)
                    .build();
            reporter1.start(interval, TimeUnit.SECONDS);
        }
    }

    public static void addCacheGauges(MetricRegistry metrics, String name, Cache cache) {
        try {
            metrics.register(name + "cache-size", (Gauge<Long>) () -> cache.estimatedSize());
            metrics.register(name + "evictions", (Gauge<Long>) () -> cache.stats().evictionCount());
            metrics.register(name + "hit-rate", (Gauge<Double>) () -> cache.stats().hitRate());
            metrics.register(name + "hits", (Gauge<Long>) () -> cache.stats().hitCount());
            metrics.register(name + "misses", (Gauge<Long>) () -> cache.stats().missCount());
        } catch (IllegalArgumentException e) {
            // Re-registering metrics during test runs, not a problem
        }
    }


    public static void addJvmMetrics(MetricRegistry metrics, String pfx) {
        try {
            metrics.register(pfx + "jvm.gc", metricsJVMGC);
            metrics.register(pfx + "jvm.memory", metricsJVMMem);
            metrics.register(pfx + "jvm.thread", metricsJVMThread);
            metrics.register(pfx + "jvm.file-descriptors-used", metricsJVMFdGauge);
        } catch (IllegalArgumentException e) {
            // Re-registering metrics during test runs, not a problem
        }
    }

    public static Timer.Context getConditionalContext(Timer t) {
        return getConditionalContext(metricsCollectionEnabled, t);
    }

    public static Timer.Context getConditionalContext(boolean enabled, Timer t) {
        return enabled ? t.time() : null;
    }

    public static void stopConditionalContext(Timer.Context context) {
        if (context != null) {
            context.stop();
        }
    }

    public static void incConditionalCounter(boolean enabled, Counter counter, long amount) {
        if (enabled) {
            counter.inc(amount);
        }
    }
}
