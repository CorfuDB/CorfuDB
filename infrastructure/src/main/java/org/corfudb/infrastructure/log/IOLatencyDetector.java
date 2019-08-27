package org.corfudb.infrastructure.log;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import groovy.json.internal.IO;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

import org.apache.maven.settings.Server;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

@Slf4j
public class IOLatencyDetector {
    static boolean spikeDetected;
    static int thresh;
    static int maxLatency;
    static int pagesize = ( 1<< 12);
    Timer writeMetrics;
    Timer readMetrics;
    Timer syncMetrics;

    public IOLatencyDetector(ServerContext serverContext, int threshVal, int maxLatencyVal) {
        spikeDetected = false;
        thresh = threshVal;
        maxLatency = maxLatencyVal;
        //xq todo: define good name for metrics
        readMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":readMetrics");
        writeMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":writeMetrics");
        syncMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":syncMetrics");
    }

    public IOLatencyDetector(int threshVal, int maxLatencyVal, String writeMetricsName, String readMetricsName) {
        spikeDetected = false;
        thresh = threshVal;
        maxLatency = maxLatencyVal;
        //xq todo: define good name for metrics
        readMetrics = ServerContext.getMetrics ().timer (writeMetricsName + ":readMetrics");
        writeMetrics = ServerContext.getMetrics ().timer (readMetricsName + ":writeMetrics");
        syncMetrics = ServerContext.getMetrics ().timer (readMetricsName + ":syncMetrics");
    }

    public static void metricsHis(IOLatencyDetector ioLatencyDetector) {
        metricsHis (ioLatencyDetector.getReadMetrics ());
        metricsHis (ioLatencyDetector.getWriteMetrics ());
    }


    public Timer getWriteMetrics() {
        return writeMetrics;
    }

    public Timer getReadMetrics() {
        return readMetrics;
    }

    public Timer getSyncMetrics() {
        return syncMetrics;
    }

    public static void update(Timer timer, long start, int size) {
        int numUnit = (size + pagesize - 1) >> 12;
        long dur = (System.nanoTime () - start) / numUnit;

        if ((dur > (timer.getSnapshot ().getMean () * thresh)) && dur > maxLatency) {
            spikeDetected = true;
            metricsHis(timer);
            log.warn ("spikeDetected");
        } else if (spikeDetected && dur < timer.getSnapshot ().getMean ()) {
            spikeDetected = false;
            metricsHis(timer);
            log.warn ("spike cleared");
        }
        timer.update ((System.nanoTime () - start) / numUnit, TimeUnit.NANOSECONDS);
    }

    // used by syncMetrics
    public static void update(Timer timer, long start) {
        long dur = (System.nanoTime () - start);

        if ((dur > (timer.getSnapshot ().getMean () * thresh)) && dur > maxLatency) {
            spikeDetected = true;
            metricsHis(timer);
            log.warn ("spikeDetected");
        } else if (spikeDetected && dur < timer.getSnapshot ().getMean ()) {
            spikeDetected = false;
            metricsHis(timer);
            log.warn ("spike cleared");
        }
        timer.update (System.nanoTime () - start, TimeUnit.NANOSECONDS);
    }

    public static boolean reportSpike() {
        return spikeDetected;
    }

    static public void metricsHis(Timer timer) {
        log.info ("Timer {} Count {} Latency mean: {} ms  50%: {} ms  95%: {} ms  99%: {} ms",
                timer.toString (),
                timer.getCount (),
                timer.getSnapshot ().getMean ()/1000000,
                timer.getSnapshot ().get75thPercentile ()/1000000,
                timer.getSnapshot ().get95thPercentile ()/1000000,
                timer.getSnapshot ().get99thPercentile ()/1000000);
        System.out.println ("Timer " + timer.toString () + " Count " + timer.getCount () + " Latency mean " +timer.getSnapshot ().getMean ()/1000000 +" ms" +
                " 75%: " + timer.getSnapshot ().get75thPercentile ()/1000000 + " ms " + "  99%: " + timer.getSnapshot ().get99thPercentile ()/1000000 +" ms");
    }
}