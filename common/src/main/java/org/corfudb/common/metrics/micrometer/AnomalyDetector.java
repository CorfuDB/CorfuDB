package org.corfudb.common.metrics.micrometer;

import java.time.Instant;
import java.util.*;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;
import java.util.concurrent.ThreadLocalRandom;

import com.yahoo.egads.models.adm.DBScanModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AnomalyDetector {

    public Map<Long, Long> pingLatencyMap = new LinkedHashMap<>();

    private static TimeSeries expectedTimeSeries = new TimeSeries();

    private Properties p;

    // Nanosecond
    private final static Long FAILURE_DETECTOR_PING_LATENCY_UPPER_BOUND = 7000000000L;

    // Nanosecond
    private final static Long FAILURE_DETECTOR_PING_LATENCY_LOWER_BOUND = 1000000000L;

    public AnomalyDetector() {
        p = new Properties();
        setProperty(p);
    }

    public void anomalyDetection() {
        try {
            TimeSeries series = new TimeSeries();
            for (Map.Entry<Long, Long> entry: pingLatencyMap.entrySet()) {
                series.append(entry.getKey(), entry.getValue());
            }
            initializeExpectedTimeSeries();
            DBScanModel dbs = new DBScanModel(p);
            dbs.tune(series.data, expectedTimeSeries.data);
            Anomaly.IntervalSequence anomalies = dbs.detect(series.data, expectedTimeSeries.data);
            for (int i = 0; i < anomalies.size(); i++) {
                log.info("High latency detected from {}, to {}...Corfu won't be stable", Instant.ofEpochMilli(anomalies.get(i).startTime).toString(),
                        Instant.ofEpochMilli(anomalies.get(i).endTime).toString());
            }
        } catch (Exception e) {
            log.error("fail to detect");
        }
    }

    private void setProperty(Properties p) {
        p.setProperty("DETECTION_WINDOW_START_TIME", "0");
        p.setProperty("MAX_ANOMALY_TIME_AGO",  "999999999");
        p.setProperty("WINDOW_SIZE", "0.1");
        p.setProperty("AGGREGATION", "1");
        p.setProperty("OP_TYPE",	"DETECT_ANOMALY");
        p.setProperty("TS_MODEL", "NaiveModel");
        p.setProperty("AD_MODEL", "DBScanModel");
        p.setProperty("INPUT", "STDIN");
        p.setProperty("OUTPUT", "STD_OUT");
        p.setProperty("AUTO_SENSITIVITY_ANOMALY_PCNT", "0.01");
        p.setProperty("AUTO_SENSITIVITY_SD", "3.0");
    }

    private void initializeExpectedTimeSeries() throws Exception {
        for (Map.Entry<Long, Long> entry: pingLatencyMap.entrySet()) {
            expectedTimeSeries.append(entry.getKey(), ThreadLocalRandom.current().nextLong(FAILURE_DETECTOR_PING_LATENCY_LOWER_BOUND, FAILURE_DETECTOR_PING_LATENCY_UPPER_BOUND));
        }
        log.info("lap hoi see latency map {}", pingLatencyMap);
        log.info("lap hoi see expected ts {}", expectedTimeSeries);
    }
}
