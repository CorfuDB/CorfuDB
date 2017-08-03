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

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MetricsUtils {
    private static final FileDescriptorRatioGauge metricsJVMFdGauge =
            new FileDescriptorRatioGauge();
    private static final MetricSet metricsJVMGC = new GarbageCollectorMetricSet();
    private static final MetricSet metricsJVMMem = new MemoryUsageGaugeSet();
    private static final MetricSet metricsJVMThread = new ThreadStatesGaugeSet();

    private static Properties metricsProperties = new Properties();
    @Getter
    private static boolean metricsCollectionEnabled = true;
    private static boolean metricsReportingEnabled = false;
    private static String mpTrigger = "filter-trigger"; // internal use only

    /**
     * Load a metrics properties file.
     *
     * <p>The expected properties in this properties file are:
     * <ul>
     * <li> collection-enabled: Boolean for whether metrics collection is enabled.
     * <li> reporting-enabled: Boolean for whether CSV output will be generated.
     * <li> directory: String for the path to the CSV output subdirectory
     * <li> nterval: Long for the reporting interval for CSV output
     * </ul>
     *
     * <p>For each reporting interval, this function will be
     * called to re-parse the properties file and to
     * re-evaluate the value of 'collection-enabled' and
     * 'reporting-enabled'.  Changes to any other property
     * in this file will be ignored.
     */
    private static void loadPropertiesFile() {
        String propPath;

        if ((propPath = System.getenv("METRICS_PROPERTIES")) != null) {
            try (FileInputStream is = new FileInputStream(propPath)) {
                metricsProperties.load(is);
                metricsCollectionEnabled = Boolean.valueOf((String) metricsProperties
                        .get("collection-enabled"));
                metricsReportingEnabled = Boolean.valueOf((String) metricsProperties
                        .get("reporting-enabled"));
            } catch (Exception e) {
                log.error("Error processing METRICS_PROPERTIES {}: {}", propPath,
                        e.toString());
            }
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
     * in loadPropertiesFile()'s docs.  The report interval and report
     * directory cannot be altered at runtime.
     *
     * @param metrics Metrics registry
     */
    public static void metricsReportingSetup(MetricRegistry metrics) {
        metrics.counter(mpTrigger);
        loadPropertiesFile();
        String outPath = (String) metricsProperties.get("directory");
        if (outPath != null && !outPath.isEmpty()) {
            Long interval = Long.valueOf((String) metricsProperties.get("interval"));
            File statDir = new File(outPath);
            statDir.mkdirs();
            MetricFilter f = (name, metric) -> {
                if (name.equals(mpTrigger)) {
                    loadPropertiesFile();
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
