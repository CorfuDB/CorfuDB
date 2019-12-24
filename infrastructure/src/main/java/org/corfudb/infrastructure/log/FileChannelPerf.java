package org.corfudb.infrastructure.log;


import com.codahale.metrics.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FileChannelPerf implements Closeable {

    @Getter
    static Timer writeMetrics;
    @Getter
    static Timer readMetrics;
    @Getter
    static Timer syncMetrics;
    @Getter
    static Timer syncMetaMetrics;

    static final int MB_DATA = (1 << 20);
    static final String READ_METRICS = "FileReadMetrics";
    static final String WRITE_METRICS = "FileWriteMetrics";
    static final String SYNC_METRICS = "FileSyncMetrics";
    static final String SYNC_META_METRICS = "FileMetaSyncMetrics";
    static boolean detectorEnabled;
    static Duration spikeLatency;
    static SlidingWindow slidingWindow;
    private FileChannel fileChannel;


    public static void setupParameters(final Duration cntSpike, final Duration maxLatencyVal, final boolean report) {
        detectorEnabled = report;
        spikeLatency = maxLatencyVal;
        readMetrics = ServerContext.getMetrics().timer(READ_METRICS);
        writeMetrics = ServerContext.getMetrics().timer(WRITE_METRICS);
        syncMetrics = ServerContext.getMetrics().timer(SYNC_METRICS);
        syncMetaMetrics = ServerContext.getMetrics().timer(SYNC_META_METRICS);
        slidingWindow = new SlidingWindow(cntSpike.getSeconds(), cntSpike.getSeconds(), maxLatencyVal.getSeconds());
    }

    private static long getUnit(long size) {
        return(size + MB_DATA - 1) / MB_DATA;
    }

    public FileChannelPerf(FileChannel fc) {
        this.fileChannel = fc;
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
        return fileChannel.size();
    }

    public long position() throws IOException {
        return fileChannel.position();
    }

    public FileChannelPerf position(long address) throws IOException {
        fileChannel.position(address);
        return this;
    }

    public int read(ByteBuffer buf, boolean use_position, long position) throws IOException {
        final long start = System.nanoTime();
        final int size = buf.remaining();
        int res = 0;

        if(!use_position) {
            res = fileChannel.read(buf);
        } else {
            res = fileChannel.read(buf, position);
        }
        update(readMetrics, READ_METRICS, start, size);
        return res;
    }

    public int read(ByteBuffer dst) throws IOException {
        return read(dst, false, 0);
    }

    public int read(ByteBuffer dst, long position) throws IOException {
        return read(dst, true, position);
    }

    public int write(ByteBuffer buf, boolean use_position, long position) throws IOException {
        long start = System.nanoTime();
        int size = buf.remaining();
        int res = 0;
        if(!use_position)
            res = fileChannel.write(buf);
        else {
            res = fileChannel.write(buf, position);
        }
        update(writeMetrics, WRITE_METRICS, start, size);
        return res;
    }

    public int write(ByteBuffer dst) throws IOException {
        return write(dst, false, 0);
    }

    public int write(ByteBuffer dst, long position) throws IOException {
        return write(dst, true, position);
    }

    public void force(boolean force) throws IOException {
        final long start = System.nanoTime();
        fileChannel.force(force);

        if(force)
            update(syncMetaMetrics, SYNC_META_METRICS, start, MB_DATA);
        else
            update(syncMetrics, SYNC_METRICS, start, MB_DATA);
    }

    public static void metricsHis(Timer timer, String name) {
        log.info("Timer {} Count {} Latency mean: {} ms  50%: {} ms  95%: {} ms  99%: {} ms",
                timer,
                timer.getCount(),
                timer.getSnapshot().getMean() / 1000000,
                timer.getSnapshot().get75thPercentile() / 1000000,
                timer.getSnapshot().get95thPercentile() / 1000000,
                timer.getSnapshot().get99thPercentile() / 1000000);
    }

    public static void metricsHis() {
        metricsHis(readMetrics, READ_METRICS);
        metricsHis(writeMetrics, WRITE_METRICS);
        metricsHis(syncMetrics, SYNC_METRICS);
        metricsHis(syncMetaMetrics, SYNC_META_METRICS);
    }

    public final boolean isOpen() {
        return fileChannel.isOpen();
    }

    public FileChannelPerf truncate(final long size) throws IOException {
        fileChannel.truncate(size);
        return this;
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public static synchronized void update(Timer timer, String name, long start, long size) {
        long dur = (System.nanoTime() - start);
        long durSec = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

        if (durSec > spikeLatency.getSeconds() * getUnit(size)) {
            log.warn("There is a spike: {} {} MB data in {} seconds.", name, getUnit(size), durSec);
        }
        slidingWindow.update(durSec);
        timer.update(dur, TimeUnit.NANOSECONDS);
    }

    public static boolean reportSpike() {

        if (detectorEnabled && slidingWindow.report())
            return true;
        return false;
    }
}
