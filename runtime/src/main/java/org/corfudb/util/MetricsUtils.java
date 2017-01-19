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
    static Properties metricsProperties = new Properties();
    static boolean metricsReportingEnabled = false;
    @Getter
    static private final String mpTrigger = "filter-trigger"; // internal use only

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

    public static void metricsReportingSetup(MetricRegistry metrics) {
        loadPropertiesFile();
        String outPath = (String) metricsProperties.get("directory");
        if (outPath != null && !outPath.isEmpty()) {
            Long interval = Long.valueOf((String) metricsProperties.get("interval"));
            File statDir = new File(outPath);
            statDir.mkdirs();
            MetricFilter f = new MetricFilter() {
                @Override
                public boolean matches(String name, Metric metric) {
                    if (name.equals(mpTrigger)) {
                        loadPropertiesFile();
                        return false;
                    }
                    return metricsReportingEnabled;
                }
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
