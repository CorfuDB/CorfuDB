package org.corfudb.common.metrics.micrometer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.yahoo.egads.data.Anomaly;
import com.yahoo.egads.data.TimeSeries;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;

import com.yahoo.egads.models.adm.DBScanModel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AnomalyDetector {

    public Map<Long, Long> pingLatencyMap = new ConcurrentSkipListMap<>();

    private static TimeSeries expectedTimeSeries = new TimeSeries();

    private Properties p;

    // Nanosecond
    private static Long FAILURE_DETECTOR_PING_LATENCY_UPPER_BOUND = 22000000L;

    // Nanosecond
    private static Long FAILURE_DETECTOR_PING_LATENCY_LOWER_BOUND = 1000000L;

    private static Long FAILURE_DETECTOR_PING_LATENCY_POSITIVE_THRESOLD = 100000000L;

    public AnomalyDetector() {
        p = new Properties();
        setProperty(p);
    }

    public void anomalyDetection() {
        int falsePositive = 0;
        Instant start = Instant.now();
        try {
            TimeSeries series = new TimeSeries();
            TimeSeries wrongSeries = new TimeSeries();
            for (Map.Entry<Long, Long> entry: pingLatencyMap.entrySet()) {
                series.append(entry.getKey(), entry.getValue());
                wrongSeries.append(entry.getKey(), ThreadLocalRandom.current().nextLong(1000000L, 3000000000L));
            }
            initializeExpectedTimeSeries();
            DBScanModel dbs = new DBScanModel(p);
            dbs.tune(series.data, expectedTimeSeries.data);
            Anomaly.IntervalSequence anomalies = dbs.detect(series.data, expectedTimeSeries.data);
            Anomaly.IntervalSequence anomalies2 = dbs.detect(wrongSeries.data, expectedTimeSeries.data);
            for (int i = 0; i < anomalies.size(); i++) {
                log.info("High latency detected at {}...Corfu won't be stable", new Date(anomalies.get(i).startTime).toInstant());
            }
            for (int i = 0; i < anomalies2.size(); i++) {
                if (anomalies2.get(i).actualVal < FAILURE_DETECTOR_PING_LATENCY_POSITIVE_THRESOLD) {
                    log.info("------This is false positive-------");
                    falsePositive += 1;
                }
                log.info("High latency detected at {} with {} nanosecond...Corfu won't be stable", new Date(anomalies2.get(i).startTime).toInstant(), anomalies2.get(i).actualVal);
            }
            if (anomalies2.size() != 0) {
                log.info("False positive rate is {}", (float) falsePositive / (float) anomalies2.size());
            }
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            log.info("The detection takes {} seconds", (float)timeElapsed.toNanos()/1000000000.0f);
        } catch (Exception e) {
            log.error("fail to detect");
        }
    }

    private void setProperty(Properties p) {
        p.setProperty("DETECTION_WINDOW_START_TIME", "0");
        p.setProperty("MAX_ANOMALY_TIME_AGO",  "999999999");
        p.setProperty("AGGREGATION", "1");
        p.setProperty("OP_TYPE", "DETECT_ANOMALY");
        p.setProperty("AD_MODEL", "DBScanModel");
        p.setProperty("INPUT", "STDIN");
        p.setProperty("OUTPUT", "STD_OUT");
        p.setProperty("AUTO_SENSITIVITY_ANOMALY_PCNT", "0.001");
        p.setProperty("AUTO_SENSITIVITY_SD", "3.0");
    }

    private void initializeExpectedTimeSeries() throws Exception{
        for (Map.Entry<Long, Long> entry : pingLatencyMap.entrySet()){
            if (expectedTimeSeries.size() == 0) {
                expectedTimeSeries.append(entry.getKey(), ThreadLocalRandom.current().nextLong(FAILURE_DETECTOR_PING_LATENCY_LOWER_BOUND, FAILURE_DETECTOR_PING_LATENCY_UPPER_BOUND));
            }
            else if (expectedTimeSeries.lastTime() < entry.getKey()) {
                expectedTimeSeries.append(entry.getKey(), ThreadLocalRandom.current().nextLong(FAILURE_DETECTOR_PING_LATENCY_LOWER_BOUND, FAILURE_DETECTOR_PING_LATENCY_UPPER_BOUND));
            }
        }
    }
}
