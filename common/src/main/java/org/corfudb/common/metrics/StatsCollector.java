package org.corfudb.common.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * Created by Maithem on 7/7/20.
 */
@Slf4j
public class StatsCollector {

    /**     * system level metrics,

     *
     *
     * System Metrics
     * cat /proc/diskstats
     *      * network tx/rx
     *      * disk read/write
     *      * number open fds
     *      resident memory
     *           * cpu utilization
     *      * load average
     *      res memory used by process
     *
     * JVM Metrics
     *sanatize
     * gc pauses
     *
     * Server Metrics
     *     * log disk latency
     *      * read/write meter
     *      * number of holes
     *      * number of overwrites
     *      * batch queue size
     *      multi read sizes

     *      * prefix trim stats
     *      * open segment handles
     *      *
     *      * number of threads
     *      number of opened streams
     *           * cluster peer network stats
     *                * epoch changes
     *      * layout commit latency?
     *      * time it takes to achieve concensus
     *      * seal time?
     *           * FD period time
     *      * bytes freed
     *      * delete time for prefix trim
     *      *
     *      * logunit/client cache stats
     *      *
     *      * fsync time
     *      *
     *      *
     *      * workflow time
     *      *
     *      * state transfer times?
     *      *tail query latency
     *
     *
     * compression ratio
     *
     * sequence number of streams
     *
     * Component
     * RPC
     *
     *
     *
     * Runtime Metrics
     *
     * number of open ojects in runtime
     * number of opened streams
     *
     *
     * Transaction level metrics
     *
     *
     * number of network exceptions
     *
     * direct access
     *
     * scan and filter latency
     *
     * //track lock waiting?
     *
     *
     * runtime exceptions


     *
     * flush on jvm exit?
     *
     * table level metrics
     * table size
     * VLO metrics
     * Stream Metrics
     * RuntimeGC metrics
     * serialization time?
     *
     * number of layout requests
     * number of timeouts
     * network stats rpc latency
     **
     */
    ScheduledExecutorService statsLogger = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("StatsCollector-%d")
            .build());


    Map<String, StatsGroup> statsGroups = new ConcurrentHashMap<>();
    private final SimpleDateFormat sdf;
    public StatsCollector() {
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        Runnable task = () -> {
            try (Writer writer = new StringWriter()){
                collect(writer);
                writer.flush();
                log.info("StatsCollector\n{}", writer.toString());
            } catch (Exception e) {
                log.error("Failed to collect stats", e);
            }
        };

        statsLogger.scheduleAtFixedRate(task, 60, 60, TimeUnit.SECONDS);
    }


    String now() {
        return sdf.format(new Date(System.currentTimeMillis()));
    }

    String name(String prefix, String name) {
        return prefix + "_" + name;
    }

    public void collect(Writer writer) throws IOException {
        Stack<StatsGroup> stack = new Stack<>();
        stack.addAll(statsGroups.values());
        SortedSet<String> lines = new TreeSet<>();

        while (!stack.empty()) {
            StatsGroup group = stack.pop();
            String prefix = group.getPrefix();
            group.getCounters().forEach((k, v) -> lines.add(now() + " counter " + name(prefix, k) + " " + toString(v)));
            group.getGauges().forEach((k, v) -> lines.add(now() + " gauge " + name(prefix, k) + " " + toString(v)));
            group.getHistograms().forEach((k, v) -> lines.add(now() + " histogram " + name(prefix, k) + " " + toString(v)));
            group.getMeters().forEach((k, v) -> lines.add(now() + " meter " + name(prefix, k) + " " + toString(v)));
            stack.addAll(group.getScopes().values());
        }

        for(String line : lines) {
            writer.write(line);
            writer.append("\n");
        }
    }

    private String toString(Counter counter) {
        return String.valueOf(counter.sumThenReset());
    }

    private String toString(Gauge gauge) {
        return String.valueOf(gauge.getValue());
    }

    private String toString(Histogram histogram) {
        StringBuilder sb = new StringBuilder();
        histogram.snapshotAndReset();
        sb.append("min ").append(histogram.getMin()).append(" ");
        sb.append("max ").append(histogram.getMax()).append(" ");
        sb.append("mean ").append(histogram.getMean()).append(" ");
        sb.append("50pct ").append(histogram.getMedian()).append(" ");
        sb.append("75pct ").append(histogram.get75thPercentile()).append(" ");
        sb.append("95pct ").append(histogram.get95thPercentile()).append(" ");
        sb.append("99pct ").append(histogram.get99thPercentile()).append(" ");
        return sb.toString();
    }

    private String toString(Meter meter) {
        meter.snapshotAndReset();
        return " countRate " + meter.getCountRate() + " valueRate " + meter.getValueRate();
    }

    public void register(StatsGroup statsGroup) {
        statsGroups.merge(statsGroup.getPrefix(), statsGroup, (k, v) -> {
            throw new IllegalStateException(statsGroup.getPrefix() + " already exists!");
        });
    }

    // Report
    // Raise Alarm ?
}
