package org.corfudb.common.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.slf4j.Logger;

/** Created by Maithem on 7/7/20. */
public class StatsCollector {

  private static final String LOGGER_NAME = "org.corfudb.metrics";

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(LOGGER_NAME);

  /**
   * * system level metrics,
   *
   * <p>System Metrics cat /proc/diskstats * network tx/rx * disk read/write * number open fds
   * resident memory * cpu utilization * load average res memory used by process
   *
   * <p>JVM Metrics sanatize gc pauses number of threads
   *
   * <p>Server Metrics
   *
   * <p>// admin client (seal, push layout, find new layout?) * epoch changes * layout commit
   * latency? * time it takes to achieve concensus * seal time? * FD period time * workflow time *
   * state transfer times?
   *
   * <p>compression ratio Component RPC * * // opened connect/connections/active connections * *
   * cluster peer network stats
   *
   * <p>client cache stats Runtime Metrics
   *
   * <p>number of open ojects in runtime number of opened streams
   *
   * <p>Transaction level metrics
   *
   * <p>number of network exceptions
   *
   * <p>direct access
   *
   * <p>scan and filter latency
   *
   * <p>//track lock waiting?
   *
   * <p>runtime exceptions
   *
   * <p>os.detected.name: linux
   *
   * <p>flush on jvm exit?
   *
   * <p>table level metrics table size VLO metrics Stream Metrics RuntimeGC metrics serialization
   * time?
   *
   * <p>number of layout requests number of timeouts network stats rpc latency *
   */
  ScheduledExecutorService statsLogger =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StatsCollector-%d").build());

  Map<String, StatsGroup> statsGroups = new ConcurrentHashMap<>();
  private final SimpleDateFormat sdf;

  public StatsCollector() {
    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

    Runnable task =
        () -> {
          try (Writer writer = new StringWriter()) {
            collect(writer);
            writer.flush();
            log.info("StatsCollector\n{}", writer.toString());
          } catch (Exception e) {
            log.error("Failed to collect stats", e);
          }
        };

    statsLogger.scheduleAtFixedRate(task, 30, 30, TimeUnit.SECONDS);
  }

  String now() {
    return sdf.format(new Date(System.currentTimeMillis()));
  }

  private String name(String prefix, String name) {
    if (prefix.isEmpty()) {
      return name;
    }

    return prefix + "_" + name;
  }

  public void collect(Writer writer) throws IOException {
    Stack<StatsGroup> stack = new Stack<>();
    stack.addAll(statsGroups.values());
    SortedSet<String> lines = new TreeSet<>();

    while (!stack.empty()) {
      StatsGroup group = stack.pop();
      String prefix = group.getPrefix();
      group
          .getCounters()
          .forEach((k, v) -> lines.add(now() + " counter " + name(prefix, k) + " " + toString(v)));
      group
          .getGauges()
          .forEach((k, v) -> lines.add(now() + " gauge " + name(prefix, k) + " " + toString(v)));
      group
          .getHistograms()
          .forEach(
              (k, v) -> lines.add(now() + " histogram " + name(prefix, k) + " " + toString(v)));
      group
          .getMeters()
          .forEach((k, v) -> lines.add(now() + " meter " + name(prefix, k) + " " + toString(v)));
      stack.addAll(group.getScopes().values());
    }

    for (String line : lines) {
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
    sb.append("count ").append(histogram.getCount()).append(" ");
    return sb.toString();
  }

  private String toString(Meter meter) {
    meter.snapshotAndReset();
    return " countRate " + meter.getCountRate() + " valueRate " + meter.getValueRate();
  }

  public void register(StatsGroup statsGroup) {
    statsGroups.merge(
        statsGroup.getPrefix(),
        statsGroup,
        (k, v) -> {
          throw new IllegalStateException(statsGroup.getPrefix() + " already exists!");
        });
  }

  // Report
  // Raise Alarm ?
}
