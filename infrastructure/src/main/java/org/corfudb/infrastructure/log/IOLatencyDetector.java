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
    static final String READ_METRICS = "FileReadMetrics";
    static final String WRITE_METRICS = "FileWriteMetrics";
    static final String SYNC_METRICS = "FileSyncMetrics";
    static boolean spikeDetected = false;
    static int thresh;
    static int maxLatency;

    @Getter
    static Timer writeMetrics;
    @Getter
    static Timer readMetrics;
    @Getter
    static Timer syncMetrics;

    static public void setupIOLatencyDetector(int threshVal, int maxLatencyVal) {
        spikeDetected = false;
        thresh = threshVal;
        maxLatency = maxLatencyVal;
        readMetrics = ServerContext.getMetrics ().timer (READ_METRICS);
        writeMetrics = ServerContext.getMetrics ().timer (WRITE_METRICS);
        syncMetrics = ServerContext.getMetrics ().timer (SYNC_METRICS);
        System.out.println ("start metrics size " + MetricsUtils.sizeOf.deepSizeOf(ServerContext.getMetrics ()));
    }

    static public void metricsHis() {
        metricsHis (readMetrics, READ_METRICS);
        metricsHis (writeMetrics, WRITE_METRICS);
        metricsHis (syncMetrics, SYNC_METRICS);
    }

    public static void update(Timer timer, String name, long start, int numUnit) {
        long dur = (System.nanoTime () - start) / numUnit;

        if ((dur > (timer.getSnapshot ().getMean () * thresh)) && dur > maxLatency) {
            spikeDetected = true;
            metricsHis(timer, name);
            log.warn (name + " spike Detected");
            System.out.println ("spikeDetected");
        } else if (spikeDetected && dur < timer.getSnapshot ().getMean ()) {
            spikeDetected = false;
            metricsHis(timer, name);
            log.warn (name + " spike cleared");
            System.out.println ("spike cleared");
        }
        timer.update(dur, TimeUnit.NANOSECONDS);
    }

    static private int getUnit(int size) {
        return (size + PAGE_SIZE - 1) / PAGE_SIZE;
    }

    static public int read(FileChannel fileChannel, ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        int res = 0;

        if (!use_position) {
            res = fileChannel.read (buf);
        } else {
            res = fileChannel.read(buf, position);
        }
        IOLatencyDetector.update(readMetrics, READ_METRICS, start, getUnit(size));
        return res;
    }

    static public int write(FileChannel fileChannel, ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        int res = 0;
        if (!use_position)
            res = fileChannel.write(buf);
        else {
            res = fileChannel.write(buf, position);
        }
        IOLatencyDetector.update(writeMetrics, WRITE_METRICS, start, getUnit (size));
        return res;
    }

    static public void force(FileChannel fileChannel, boolean force) throws IOException {
        long start = System.nanoTime ();
        fileChannel.force (force);
        IOLatencyDetector.update(syncMetrics, SYNC_METRICS, start, 1);
    }


    static public  boolean reportSpike() {
        return spikeDetected;
    }

    static public void metricsHis(Timer timer, String name) {
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