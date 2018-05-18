package org.corfudb.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetricsUtils {
    private static final FileDescriptorRatioGauge metricsJVMFdGauge =
            new FileDescriptorRatioGauge();
    private static final MetricSet metricsJVMGC = new GarbageCollectorMetricSet();
    private static final MetricSet metricsJVMMem = new MemoryUsageGaugeSet();
    private static final MetricSet metricsJVMThread = new ThreadStatesGaugeSet();

    private static final String PROPERTY_CSV_INTERVAL = "corfu.metrics.csv.interval";
    private static final String PROPERTY_CSV_FOLDER = "corfu.metrics.csv.folder";
    private static final String PROPERTY_JMX_REPORTING = "corfu.metrics.jmxreporting";
    private static final String PROPERTY_LOG_INTERVAL = "corfu.metrics.log.interval";
    private static final String PROPERTY_METRICS_COLLECTION = "corfu.metrics.collection";

    private static final String ADDRESS_SPACE_METRIC_PREFIX = "corfu.runtime.as-view";
    private static final MetricFilter ADDRESS_SPACE_FILTER =
            (name, metric) -> !name.contains(ADDRESS_SPACE_METRIC_PREFIX);

    private static long metricsCsvInterval;
    private static long metricsLogInterval;
    private static String metricsCsvFolder;
    @Getter
    private static boolean metricsCollectionEnabled = false;
    private static boolean metricsCsvReportingEnabled = false;
    private static boolean metricsJmxReportingEnabled = false;
    private static boolean metricsSlf4jReportingEnabled = false;
    private static String mpTrigger = "filter-trigger"; // internal use only


    /**
     * Load metrics properties.
     *
     * <p>The expected properties from vm system properties are:
     * <ul>
     * <li> metricsCollectionEnabled: Boolean taken from vm corfu.metrics.collection
     * property to enable the collection
     * <li> metricsJmxReportingEnabled: Boolean taken from vm corfu.metrics.jmxreporting
     * property to enable jmx reporting of metrics
     * <li> metricsLogInterval: Integer taken from vm corfu.metrics.log.interval
     * property for enabling and setting the intervals of reporting to log output
     * </ul>
     *
     * <p>This function will be called to set the value of metricsCollectionEnabled,
     * metricsJmxReportingEnabled, and metricsLogInterval for reporting. The value
     * of metricsCollectionEnabled represents the expected collection status.
     * Slf4j reporting is enabled if metricsLogInterval is set to a positive integer
     * representing emission intervals in seconds.
     * Jmx reporting is enabled if metricsJmxReportingEnabled is set to {@code true}.
     */
    private static void loadVmProperties() {
        metricsCollectionEnabled = Boolean.valueOf(System.getProperty(PROPERTY_METRICS_COLLECTION));
        metricsJmxReportingEnabled = Boolean.valueOf(System.getProperty(PROPERTY_JMX_REPORTING));
        try {
            metricsLogInterval = Long.valueOf(System.getProperty(PROPERTY_LOG_INTERVAL));
            metricsSlf4jReportingEnabled = metricsLogInterval > 0 ? true : false;
        } catch (NumberFormatException e) {
            log.warn("Extracting metrics log reporting interval property failed. " +
                    "Reporting to corfu metrics log is disabled");
            metricsSlf4jReportingEnabled = false;
        }

        try {
            metricsCsvInterval = Long.valueOf(System.getProperty(PROPERTY_CSV_INTERVAL));
            metricsCsvFolder = String.valueOf(System.getProperty(PROPERTY_CSV_FOLDER));
            metricsCsvReportingEnabled = metricsCsvInterval > 0 ? true : false;
        } catch (NumberFormatException e) {
            log.warn("Extracting metrics CSV reporting interval property failed. " +
                    "Reporting to corfu metrics csv files is disabled");
            metricsCsvReportingEnabled = false;
        }

        if (!metricsCollectionEnabled) {
            metricsJmxReportingEnabled = false;
            metricsSlf4jReportingEnabled = false;
            metricsCsvReportingEnabled = false;
            log.info("Corfu metrics collection and all reporting types are disabled");
        }
    }

    /**
     * Check if the metricsReportingSetup() function has been called
     * on 'metrics' before now.
     *
     * @param metrics Metric Registry
     * @return True if metricsReportingSetup() function has been called earlier
     */
    public static boolean isMetricsReportingSetUp(@NonNull MetricRegistry metrics) {
        return metrics.getNames().contains(mpTrigger);
    }

    /**
     * Start metrics reporting via the Dropwizard 'CsvReporter', 'JmxReporter',
     * and 'Slf4jReporter'. Reporting can be turned on and off via the system
     * properties described in loadVmProperties()'s docs.
     * The report interval and report directory cannot be altered at runtime.
     *
     * @param metrics Metrics registry
     */
    public static void metricsReportingSetup(@NonNull MetricRegistry metrics) {
        if (isMetricsReportingSetUp(metrics)) return;

        metrics.counter(mpTrigger);
        loadVmProperties();

        if (metricsCollectionEnabled) {
            setupCsvReporting(metrics);
            setupJmxReporting(metrics);
            setupSlf4jReporting(metrics);
        }
    }

    // If enabled, setup jmx reporting
    private static void setupJmxReporting(MetricRegistry metrics) {
        if (!metricsJmxReportingEnabled) return;

        // This filters noisy addressSpace metrics to have a clean JMX reporting
        JmxReporter jmxReporter = JmxReporter.forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .inDomain("corfu.metrics")
                .filter(ADDRESS_SPACE_FILTER)
                .build();
        jmxReporter.start();
    }

    // If enabled, setup slf4j reporting
    private static void setupSlf4jReporting(MetricRegistry metrics) {
        if (!metricsSlf4jReportingEnabled) return;

        MetricFilter mpTriggerFilter = (name, metric) -> !name.equals(mpTrigger);

        Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .outputTo(LoggerFactory.getLogger("org.corfudb.metricsdata"))
                .filter(mpTriggerFilter)
                .build();
        slf4jReporter.start(metricsLogInterval, TimeUnit.SECONDS);
    }

    // If enabled, setup csv reporting
    private static void setupCsvReporting(MetricRegistry metrics) {
        if (!metricsCsvReportingEnabled) return;

        final File directory = new File(metricsCsvFolder);

        if (!directory.isDirectory() ||
            !directory.exists() ||
            !directory.canWrite()) {
            log.warn("Provided CSV directory : {} doesn't exist, isn't a directory, or not accessible " +
                    "for writes. Disabling CSV Reporting.");
            metricsCsvReportingEnabled = false;
            return;
        }

        CsvReporter csvReporter = CsvReporter.forRegistry(metrics)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .filter(ADDRESS_SPACE_FILTER)
                .build(directory);
        csvReporter.start(metricsCsvInterval, TimeUnit.SECONDS);
    }

    public static void addCacheGauges(@NonNull MetricRegistry metrics,
                                      @NonNull String name,
                                      @NonNull Cache cache) {
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

    public static void addJvmMetrics(@NonNull MetricRegistry metrics, String pfx) {
        try {
            metrics.register(pfx + "jvm.gc", metricsJVMGC);
            metrics.register(pfx + "jvm.memory", metricsJVMMem);
            metrics.register(pfx + "jvm.thread", metricsJVMThread);
            metrics.register(pfx + "jvm.file-descriptors-used", metricsJVMFdGauge);
        } catch (IllegalArgumentException e) {
            // Re-registering metrics during test runs, not a problem
        }
    }

    public static Timer.Context getConditionalContext(@NonNull Timer t) {
        return getConditionalContext(metricsCollectionEnabled, t);
    }

    public static Timer.Context getConditionalContext(boolean enabled, @NonNull Timer t) {
        return enabled ? t.time() : null;
    }

    public static void stopConditionalContext(Timer.Context context) {
        if (context != null) {
            context.stop();
        }
    }

    public static void incConditionalCounter(boolean enabled, @NonNull Counter counter, long amount) {
        if (enabled) {
            counter.inc(amount);
        }
    }
}
