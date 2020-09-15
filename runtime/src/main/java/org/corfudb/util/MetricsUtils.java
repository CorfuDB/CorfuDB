package org.corfudb.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer.Config;
import org.ehcache.sizeof.SizeOf;
import org.slf4j.LoggerFactory;

@Slf4j
public class MetricsUtils {

    private MetricsUtils() {
        // Preventing instantiation of this utility class
    }

    private static final Gauge<Long> metricsJVMUptime =
            () -> ManagementFactory.getRuntimeMXBean().getUptime();
    private static final RatioGauge metricsJVMFdGauge = new FileDescriptorRatioGauge();
    private static final MetricSet metricsBuffPooled =
            new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer());
    private static final MetricSet metricsClassLoadingGauge = new ClassLoadingGaugeSet();
    private static final MetricSet metricsJVMGC = new GarbageCollectorMetricSet();
    private static final MetricSet metricsJVMMem = new MemoryUsageGaugeSet();
    private static final MetricSet metricsJVMThread = new ThreadStatesGaugeSet();

    // Domain prefix for reporting Corfu metrics
    private static final String CORFU_METRICS = "corfu.metrics";

    // JVM flags used for configuration of collection and reporting of metrics
    private static final String PROPERTY_CSV_FOLDER = "corfu.metrics.csv.folder";
    private static final String PROPERTY_CSV_INTERVAL = "corfu.metrics.csv.interval";
    private static final String PROPERTY_JMX_REPORTING = "corfu.metrics.jmxreporting";
    private static final String PROPERTY_JVM_METRICS_COLLECTION = "corfu.metrics.jvm";
    private static final String PROPERTY_LOG_INTERVAL = "corfu.metrics.log.interval";
    private static final String PROPERTY_LOCAL_METRICS_COLLECTION =
            "corfu.local.metrics.collection";

    private static final String ADDRESS_SPACE_METRIC_PREFIX = "corfu.runtime.as-view";
    private static final MetricFilter ADDRESS_SPACE_FILTER =
            (name, metric) -> !name.contains(ADDRESS_SPACE_METRIC_PREFIX);

    private static long metricsCsvInterval;
    private static long metricsLogInterval;
    private static String metricsCsvFolder;
    @Getter
    private static boolean metricsCollectionEnabled = false;
    private static boolean metricsLocalCollectionEnabled = false;
    private static boolean metricsCsvReportingEnabled = false;
    private static boolean metricsJmxReportingEnabled = false;
    private static boolean metricsJvmCollectionEnabled = false;
    private static boolean metricsSlf4jReportingEnabled = false;
    private static final String mpTrigger = "filter-trigger"; // internal use only

    public static final SizeOf sizeOf = SizeOf.newInstance();
    public static final int NO_METRICS_PORT = -1;

    /**
     * Load metrics properties.
     *
     * <p>The following properties can be set using jvm flags:
     * <ul>
     * <li> metricsCollectionEnabled: Boolean taken from jvm corfu.metrics.collection
     * property to enable the metrics collection.
     * <li> metricsJmxReportingEnabled: Boolean taken from jvm corfu.metrics.jmxreporting
     * property to enable jmx reporting of metrics.
     * <li> metricsJvmCollectionEnabled: Boolean taken from jvm corfu.metrics.jvm
     * property for enabling reporting on jmv metrics such as garbage collection, threads,
     * and memory consumption.
     * <li> metricsLogAnalysisEnabled: Boolean taken from jvm corfu.metrics.log.analysis
     * property for enabling reporting on logger statistics.
     * <li> metricsLogInterval: Integer taken from jvm corfu.metrics.log.interval
     * property for enabling and setting the intervals of reporting to log output (in
     * seconds). A positive value indicates the reporting is enabled at provided
     * intervals.
     * <li> metricsCsvInterval: Integer taken from jvm corfu.metrics.csv.interval
     * property for enabling and setting the intervals of reporting to csv (in seconds).
     * A positive value indicates the reporting is enabled at provided intervals.
     * <li> metricsCsvFolder: String taken from jvm corfu.metrics.csv.folder
     * property for destination path of csv reporting.
     * </ul>
     *
     * <p>This method will be called to set the value of above-mentioned properties
     * for collection and reporting. For example using the following jvm flags will
     * enable collection of corfu, jvm, and log statistics and their reporting through
     * logs, csv, and jmx.
     *
     * {@code -Dcorfu.metrics.collection=True
     * -Dcorfu.metrics.csv.interval=30
     * -Dcorfu.metrics.csv.folder=/tmp/csv5
     * -Dcorfu.metrics.jmxreporting=True
     * -Dcorfu.metrics.log.analysis=True
     * -Dcorfu.metrics.jvm=True
     * -Dcorfu.metrics.log.interval=60}
     */
    private static void loadVmProperties() {
        metricsLocalCollectionEnabled = Boolean.valueOf(System.getProperty(
                PROPERTY_LOCAL_METRICS_COLLECTION));

        metricsJmxReportingEnabled = Boolean.valueOf(System.getProperty(PROPERTY_JMX_REPORTING));
        metricsJvmCollectionEnabled = Boolean.valueOf(System.getProperty(PROPERTY_JVM_METRICS_COLLECTION));

        metricsLogInterval = Long.valueOf(System.getProperty(PROPERTY_LOG_INTERVAL, "0"));
        metricsSlf4jReportingEnabled = metricsLogInterval > 0;

        metricsCsvInterval = Long.valueOf(System.getProperty(PROPERTY_CSV_INTERVAL, "0"));
        metricsCsvFolder = String.valueOf(System.getProperty(PROPERTY_CSV_FOLDER));
        metricsCsvReportingEnabled = metricsCsvInterval > 0;
    }

    /**
     * Check whether the metrics reporting has been already set up using metricsReportingSetup.
     *
     * @param metrics Metric Registry
     * @return a boolean representing whether metrics reporting has been already set up.
     * earlier
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
        if (isMetricsReportingSetUp(metrics)) {
            return;
        }

        metrics.counter(mpTrigger);

        loadVmProperties();

        if (metricsLocalCollectionEnabled) {
            setupCsvReporting(metrics);
            setupJvmMetrics(metrics);
            setupJmxReporting(metrics);
            setupSlf4jReporting(metrics);

            metricsCollectionEnabled = true;
            log.info("Corfu CSV metrics collection and all reporting types are enabled");
        }
    }
    /**
     * Start metrics reporting via Prometheus.
     *
     * @param metricRegistry Metrics registry
     * @param prometheusMetricsPort port on which statistics should be exported
     */
    public static void metricsReportingSetup(@NonNull MetricRegistry metricRegistry,
                                              int prometheusMetricsPort) {
        Config config = new Config(prometheusMetricsPort, Config.ENABLED);
        MetricsServer server = new PrometheusMetricsServer(config, metricRegistry);
        server.start();

        metricsCollectionEnabled = true;
        log.info("Metrics exporting via Prometheus has been enabled at port {}.",
                prometheusMetricsPort);
    }

    // If enabled, setup jmx reporting
    private static void setupJmxReporting(MetricRegistry metricRegistry) {
        if (!metricsJmxReportingEnabled) return;

        // This filters noisy addressSpace metrics to have a clean JMX reporting
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MICROSECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .inDomain(CORFU_METRICS)
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
            log.warn("Provided CSV directory : {} doesn't exist, isn't a directory, or " +
                    "not accessible for writes. Disabling CSV Reporting.", directory);
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

    // If enabled, setup reporting of JVM metrics including garbage collection,
    // memory, thread statistics, buffer, uptime, class loading, netty's direct and heap
    // allocations.
    private static void setupJvmMetrics(@NonNull MetricRegistry metrics) {
        if (!metricsJvmCollectionEnabled) return;

        try {
            metrics.register("jvm.buffers", metricsBuffPooled);
            metrics.register("jvm.class-loading", metricsClassLoadingGauge);
            metrics.register("jvm.file-descriptors-used", metricsJVMFdGauge);
            metrics.register("jvm.gc", metricsJVMGC);
            metrics.register("jvm.memory", metricsJVMMem);
            metrics.register("jvm.thread", metricsJVMThread);
            metrics.register("jvm.uptime", metricsJVMUptime);
            metrics.register("corfu.netty.mem.pooled-byte-buf.direct", getNettyPooledDirectMemGauge());
            metrics.register("corfu.netty.mem.pooled-byte-buf.heap", getNettyPooledHeapMemGauge());
        } catch (IllegalArgumentException e) {
            // Re-registering metrics during test runs, not a problem
        }
    }

    public static Timer.Context getConditionalContext(@NonNull Timer t) {
        return metricsCollectionEnabled ? t.time() : null;
    }

    public static void stopConditionalContext(Timer.Context context) {
        if (context != null) {
            context.stop();
        }
    }

    public static void incConditionalCounter(@NonNull Counter counter, long amount) {
        if (metricsCollectionEnabled) {
            counter.inc(amount);
        }
    }

    /**
     * return a gauge on direct memory used by netty's PooledByteBufAllocator
     */
    private static Gauge<Long> getNettyPooledDirectMemGauge() {
        return () -> PooledByteBufAllocator.DEFAULT.metric().usedDirectMemory();
    }

    /**
     * return a gauge on heap memory used by netty's PooledByteBufAllocator
     */
    private static Gauge<Long> getNettyPooledHeapMemGauge() {
        return () -> PooledByteBufAllocator.DEFAULT.metric().usedHeapMemory();
    }

    /**
     * This method creates object size gauge and registers it to the metrics registry. The
     * method guarantees that no strong reference to the provided object will be retained.
     * Hence it does not prevent garbage collection if the client does not hold a strong
     * reference to the object being measured.
     *
     * @param metrics the metrics registry to which the size gauge will be registered.
     * @param object the object which its size will be measured with a corresponding
     *               gauge*
     *
     * @return a gauge that provides an estimate of deep size for the provided object
     *         as long as there exist a strong reference to that object.
     */
    public static Gauge<Long> addMemoryMeasurerFor(@NonNull MetricRegistry metrics,
                                                   Object object) {
        String simpleClassName = object == null ?
                "Object" :
                object.getClass().getSimpleName();

        return  metrics.register(String.format("deep-sizeof.%s@%04x",
                                               simpleClassName,
                                               System.identityHashCode(object)),
                                 getSizeGauge(object));
    }

    /**
     * This method creates a gauge that returned the deep size estimation of an object
     * while there is a reference to it in the code. Otherwise it will return 0 as the
     * object can be claimed by Garbage collection. Note that this gauge guarantees
     * that it will not hold any strong reference to the object that it measures.
     *
     * @param object an object to which its size will be estimated using the return
     *               gauge
     * @return a gauge that provides an estimate of deep size for the provided object
     *         as long as there exist a strong reference to that object.
     */
    private static Gauge<Long> getSizeGauge(Object object) {
        WeakReference<Object> toBeMeasuredObject = new WeakReference<>(object);

        return () -> {
            final Object objectOfInterest = toBeMeasuredObject.get();
            return (objectOfInterest != null) ?
                    sizeOf.deepSizeOf(objectOfInterest) :
                    0L;
        };
    }
}
