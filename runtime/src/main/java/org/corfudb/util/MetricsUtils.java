package org.corfudb.util;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetricsUtils {
    private static Properties metricsProperties = new Properties();
    private static boolean metricsReportingEnabled = false;
    private static String mpTrigger = "filter-trigger"; // internal use only

    /**
     * Load a metrics properties file.
     * The expected properties in this properties file are:
     * <p>
     * enabled: Boolean for whether CSV output will be generated.
     * For each reporting interval, this function will be
     * called to re-parse the properties file and to
     * re-evaluate the value of 'enabled'.  Changes to
     * any other property in this file will be ignored.
     * <p>
     * directory: String for the path to the CSV output subdirectory
     * <p>
     * interval: Long for the reporting interval for CSV output
     */
    private static void loadPropertiesFile() {
        String propPath;

        if ((propPath = System.getenv("METRICS_PROPERTIES")) != null) {
            try {
                metricsProperties.load(new FileInputStream(propPath));
                metricsReportingEnabled = Boolean.valueOf((String) metricsProperties.get("enabled"));
            } catch (Exception e) {
                log.error("Error processing METRICS_PROPERTIES {}: {}", propPath, e.toString());
            }
        }
    }

    /**
     * Start metrics reporting via the Dropwizard 'CsvReporter' file writer.
     * Reporting can be turned on and off via the properties file described
     * in loadPropertiesFile()'s docs.  The report interval and report
     * directory cannot be altered at runtime.
     *
     * @param metrics
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
            CsvReporter reporter1 = CsvReporter.forRegistry(metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(f)
                    .build(statDir);
            reporter1.start(interval, TimeUnit.SECONDS);
        }
    }
}
