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
    static int spikeLatency; //in seconds
    static int spikeDuration; //in seconds
    static int cnt; //count the number of long latency period
    static long timestart;

    @Getter
    static Timer writeMetrics;
    @Getter
    static Timer readMetrics;
    @Getter
    static Timer syncMetrics;

    static public void setupIOLatencyDetector(int duration, int maxLatencyVal) {
        spikeDetected = false;
        spikeDuration = duration;
        spikeLatency = maxLatencyVal;
        readMetrics = ServerContext.getMetrics ().timer (READ_METRICS);
        writeMetrics = ServerContext.getMetrics ().timer (WRITE_METRICS);
        syncMetrics = ServerContext.getMetrics ().timer (SYNC_METRICS);
        log.debug("start metrics size " + MetricsUtils.sizeOf.deepSizeOf(ServerContext.getMetrics ()));
    }

    static public void metricsHis() {
        metricsHis (readMetrics, READ_METRICS);
        metricsHis (writeMetrics, WRITE_METRICS);
        metricsHis (syncMetrics, SYNC_METRICS);
    }

    synchronized public static void update(Timer timer, String name, long start, int numUnit) {
        long dur = (System.nanoTime () - start) / numUnit;
        long durSec = TimeUnit.SECONDS.convert (dur, TimeUnit.NANOSECONDS);

        if ( !spikeDetected  && durSec > spikeLatency) {
            spikeDetected = true;
            cnt = 1;
            timestart = System.nanoTime ();
            metricsHis(timer, name);
            log.warn (name + " spike Detected " + durSec + " seconds");
        } else if (spikeDetected && dur < spikeLatency /2 ) {
            spikeDetected = false;
            cnt = 0;
            timestart = System.nanoTime ();
            metricsHis(timer, name);
            log.warn (name + " spike cleared delay " + durSec + " seconds");
        } else if (spikeDetected) {
            cnt++;
            log.warn (name + " spike cont delay " + durSec + " seconds" + " cnt " + cnt);
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
        if (spikeDetected && (TimeUnit.SECONDS.convert (System.nanoTime () - timestart, TimeUnit.NANOSECONDS)
                >= spikeDuration))
            return spikeDetected;
        else
            return false;
    }

    static public void metricsHis(Timer timer, String name) {
        log.info ("Timer {} Count {} Latency mean: {} ms  50%: {} ms  95%: {} ms  99%: {} ms",
                timer.toString (),
                timer.getCount (),
                timer.getSnapshot ().getMean ()/1000000,
                timer.getSnapshot ().get75thPercentile ()/1000000,
                timer.getSnapshot ().get95thPercentile ()/1000000,
                timer.getSnapshot ().get99thPercentile ()/1000000);
    }
}