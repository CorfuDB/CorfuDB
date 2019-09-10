package org.corfudb.infrastructure.log;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import groovy.json.internal.IO;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.apache.maven.settings.Server;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

@Slf4j
public class IOLatencyDetector {
    static final int PAGE_SIZE = ( 1<< 12);
    static boolean spikeDetected = false;
    static int thresh;
    static int maxLatency;

    @Getter
    static Timer writeMetrics;
    @Getter
    static Timer readMetrics;
    @Getter
    static Timer syncMetrics;

    static public void setupIOLatencyDetector(ServerContext serverContext, int threshVal, int maxLatencyVal) {
        spikeDetected = false;
        thresh = threshVal;
        maxLatency = maxLatencyVal;
        //xq todo: define good name for metrics
        readMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":readMetrics");
        writeMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":writeMetrics");
        syncMetrics = ServerContext.getMetrics ().timer (serverContext.getNodeId ().toString () + ":syncMetrics");
    }

    static public void setupIOLatencyDetector(int threshVal, int maxLatencyVal, String writeMetricsName, String readMetricsName) {
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
        metricsHis (ioLatencyDetector.getSyncMetrics ());
    }

    public static void update(Timer timer, long start, int size) {
        int numUnit = (size + PAGE_SIZE - 1) >> 12;
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

    static void read(FileChannel fileChannel, ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        if (!use_position)
            fileChannel.read(buf);
        else {
            fileChannel.read(buf, position);
        }
        IOLatencyDetector.update(readMetrics, start, size);
    }

    static void write(FileChannel fileChannel, ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        if (!use_position)
            fileChannel.read(buf);
        else {
            fileChannel.read(buf, position);
        }
        IOLatencyDetector.update(writeMetrics, start, size);
    }

    static void force(FileChannel fileChannel, boolean force) throws IOException {
        long start = System.nanoTime ();
        fileChannel.force (force);
        IOLatencyDetector.update(writeMetrics, start);
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