package org.corfudb.infrastructure.log;


import com.codahale.metrics.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.util.MetricsUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FileChannelPerf implements Closeable {//implements Closeable {

    @Getter
    static Timer writeMetrics;
    @Getter
    static Timer readMetrics;
    @Getter
    static Timer syncMetrics;

    final static int MB_DATA = (1 << 20);
    static final String READ_METRICS = "FileReadMetrics";
    static final String WRITE_METRICS = "FileWriteMetrics";
    static final String SYNC_METRICS = "FileSyncMetrics";
    static boolean detectorEnabled;
    static long spikeLatency;
    static SlidingWindow slidingWindow;
    private FileChannel fileChannel;


    static public void setupParameters(int cntSpike, int maxLatencyVal, boolean report) {
        detectorEnabled = report;
        spikeLatency = maxLatencyVal;
        readMetrics = ServerContext.getMetrics ().timer (READ_METRICS);
        writeMetrics = ServerContext.getMetrics ().timer (WRITE_METRICS);
        syncMetrics = ServerContext.getMetrics ().timer (SYNC_METRICS);
        slidingWindow = new SlidingWindow(cntSpike, cntSpike, maxLatencyVal);
    }

    static private long getUnit(long size) {
        return (size + MB_DATA - 1) / MB_DATA;
    }

    public FileChannelPerf(FileChannel fc) {
        fileChannel = fc;
    }

    public static FileChannelPerf open(Path path, Set<? extends OpenOption> options,
             FileAttribute<?>... attrs) throws IOException {
        FileChannel fileChannel = FileChannel.open(path, options, attrs);
        return new FileChannelPerf(fileChannel);
    }

    public static FileChannelPerf open(Path path, OpenOption... options) throws IOException {
        FileChannel fileChannel = FileChannel.open(path, options);
        return new FileChannelPerf(fileChannel);
    }

    public long size() throws IOException {
        return fileChannel.size ();
    }

    public long position() throws IOException {
        return fileChannel.position();
    }

    public FileChannelPerf position(long address) throws IOException {
        fileChannel.position(address);
        return this;
    }

    public int read(ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        int res = 0;

        if (!use_position) {
            res = fileChannel.read (buf);
        } else {
            res = fileChannel.read (buf, position);
        }
        update(readMetrics, READ_METRICS, start, size);
        return res;
    }

    public int read(ByteBuffer dst) throws IOException {
        return read (dst, false, 0);
    }

    public int read(ByteBuffer dst, long position) throws IOException {
        return read (dst, true, position);
    }

    public int write(ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime ();
        int size = buf.remaining ();
        int res = 0;
        if (!use_position)
            res = fileChannel.write (buf);
        else {
            res = fileChannel.write (buf, position);
        }
        update(writeMetrics, WRITE_METRICS, start, size);
        return res;
    }

    public int write(ByteBuffer dst) throws IOException {
        return write (dst, false, 0);
    }


    public int write(ByteBuffer dst, long position) throws IOException {
        return write (dst, true, position);
    }

    public void force(boolean force) throws IOException {
        long start = System.nanoTime ();
        fileChannel.force (force);
        update(syncMetrics, SYNC_METRICS, start, MB_DATA);
    }

    /*public boolean isOpen() {
        return fileChannel.isOpen ();
    }*/

    static public void metricsHis(Timer timer, String name) {
        log.info ("Timer {} Count {} Latency mean: {} ms  50%: {} ms  95%: {} ms  99%: {} ms",
                timer.toString (),
                timer.getCount (),
                timer.getSnapshot ().getMean () / 1000000,
                timer.getSnapshot ().get75thPercentile () / 1000000,
                timer.getSnapshot ().get95thPercentile () / 1000000,
                timer.getSnapshot ().get99thPercentile () / 1000000);
    }

    static public void metricsHis() {
        metricsHis (readMetrics, READ_METRICS);
        metricsHis (writeMetrics, WRITE_METRICS);
        metricsHis (syncMetrics, SYNC_METRICS);
    }

    final public boolean isOpen() {
        return fileChannel.isOpen ();
    }

    final public FileChannelPerf truncate(long size) throws IOException {
        fileChannel.truncate(size);
        return this;
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    synchronized public static void update(Timer timer, String name, long start, long size) {
        long dur = (System.nanoTime () - start);
        long durSec = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (durSec > spikeLatency*getUnit(size)) {
            log.warn("There is a spike: {} {} MB data in {} seconds.", name, getUnit(size), durSec);
        }
        slidingWindow.update(durSec);
        timer.update(dur, TimeUnit.NANOSECONDS);
    }

    static public  boolean reportSpike() {
        if (detectorEnabled && slidingWindow.report()) {
            metricsHis ();
            return true;
        }
        else
            return false;
    }
}
