package org.corfudb.perf.streaming;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.corfudb.perf.Utils.wrapRunnable;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.corfudb.perf.SimulatorArguments;
import org.corfudb.perf.Utils;
import org.corfudb.runtime.CorfuRuntime;

/**
 * Sample usage: java -jar target/streaming-sim.jar --duration 1 --endpoint localhost:9000
 * --num-consumers 4 --num-runtime 4 --num-tasks 1000 --num-producers 8 --payload-size 100
 * --update-interval 2000
 */
@Slf4j
public class StreamingSimulator {

  static class Arguments extends SimulatorArguments {
    @Parameter(
        names = {"--endpoint"},
        description = "Cluster endpoint",
        required = true)
    private List<String> endpoints = new ArrayList<>();

    @Parameter(
        names = {"--num-runtime"},
        description = "Number of corfu runtimes to use",
        required = true)
    private int numRuntime;

    @Parameter(
        names = {"--num-consumers"},
        description = "Number of consumers",
        required = true)
    int numConsumers;

    @Parameter(
        names = {"--poll-period"},
        description = "Consumer poll period")
    int pollPeriod = 10;

    @Parameter(
        names = {"--num-producers"},
        description = "Number of producers",
        required = true)
    int numProducers;

    @Parameter(
        names = {"--num-tasks"},
        description = "Total number of tasks to create" + " per producer",
        required = true)
    int numTasks;

    @Parameter(
        names = {"--update-interval"},
        description = "Progress update interval in ms",
        required = true)
    long statusUpdateMs;

    @Parameter(
        names = {"--payload-size"},
        description = "Size of payload in bytes",
        required = true)
    int payloadSize;

    @Parameter(
        names = {"--duration"},
        description = "Duration of time to run the simulation " + "for in minutes",
        required = true)
    int duration;
  }

  /**
   * Create a list of unique producers
   *
   * @param arguments arguments to use
   * @param rts an array of CorfuRuntime that can be used to create the streams
   * @return A list of producers
   */
  private static List<Producer> createProducers(
      final Arguments arguments, final List<CorfuRuntime> rts) {
    if (arguments.numProducers <= 0) {
      throw new IllegalArgumentException("Not enough producers!");
    }

    final byte[] payload = new byte[arguments.payloadSize];
    ThreadLocalRandom.current().nextBytes(payload);
    final List<Producer> producers = new ArrayList<>(arguments.numProducers);
    IntStream.range(0, arguments.numProducers)
        .forEach(
            idx -> {
              final UUID id = CorfuRuntime.getStreamID("producer" + idx);
              final CorfuRuntime runtime = rts.get(idx % rts.size());
              producers.add(new Producer(id, runtime, arguments.numTasks, payload));
            });
    return Collections.unmodifiableList(producers);
  }

  /**
   * Create a list of unique consumers
   *
   * @param arguments arguments to use
   * @param rts an array of CorfuRuntime that can be used to create the streams
   * @return a list of consumers
   */
  private static List<Consumer> createConsumers(
      final Arguments arguments, final List<CorfuRuntime> rts) {
    if (arguments.numConsumers == 0) {
      return Collections.emptyList();
    }

    final List<Consumer> consumers = new ArrayList<>(arguments.numConsumers);
    IntStream.range(0, arguments.numConsumers)
        .forEach(
            idx -> {
              // Since the consumer has to consume the same producer stream
              // we need to use the same stream name used by the producer
              // to generate a consistent UUID
              String name = "producer" + idx % arguments.numProducers;
              final UUID id = CorfuRuntime.getStreamID(name);
              final CorfuRuntime runtime = rts.get(idx % rts.size());
              consumers.add(new Consumer(id, runtime, arguments.numTasks, arguments.pollPeriod));
            });
    return Collections.unmodifiableList(consumers);
  }

  /**
   * Logs uncaught exceptions.
   *
   * @param thread The thread which terminated.
   * @param throwable The throwable which caused the thread to terminate.
   */
  private static void handleUncaughtException(final Thread thread, final Throwable throwable) {
    log.error(
        "handleUncaughtThread: {} terminated with throwable of type {}",
        thread.getName(),
        throwable.getClass().getSimpleName(),
        throwable);
  }

  /**
   * Creates and connects a list of different CorfuRuntimes
   *
   * @param arguments arguments to use
   * @return a list of connected CorfuRuntimes
   */
  private static List<CorfuRuntime> initializeRuntimes(final Arguments arguments) {
    List<CorfuRuntime> runtimes = new ArrayList<>(arguments.numRuntime);
    String connectionString = String.join(",", arguments.endpoints);
    for (int rtIdx = 0; rtIdx < arguments.numRuntime; rtIdx++) {
      runtimes.add(new CorfuRuntime(connectionString).setCacheDisabled(true).connect());
    }
    return Collections.unmodifiableList(runtimes);
  }

  public static void main(String[] stringArgs) throws Exception {
    final Arguments arguments = new Arguments();
    Utils.parse(arguments, stringArgs);

    // TODO(Maithem): enable multiple producers to create tasks for the same underlying stream

    final List<CorfuRuntime> rts = initializeRuntimes(arguments);

    final List<Consumer> consumers = createConsumers(arguments, rts);
    final List<Producer> producers = createProducers(arguments, rts);

    final ExecutorService producersPool =
        Executors.newFixedThreadPool(
            arguments.numProducers,
            new ThreadFactoryBuilder()
                .setNameFormat("producer-%d")
                .setUncaughtExceptionHandler(StreamingSimulator::handleUncaughtException)
                .setDaemon(true)
                .build());
    final ExecutorService consumersPool =
        Executors.newFixedThreadPool(
            arguments.numConsumers,
            new ThreadFactoryBuilder()
                .setNameFormat("consumer-%d")
                .setUncaughtExceptionHandler(StreamingSimulator::handleUncaughtException)
                .setDaemon(true)
                .build());

    long startTsNs = System.nanoTime();

    // Start the producers before the consumers and wait till they consumers are done
    producers.forEach(producer -> producersPool.submit(wrapRunnable(producer)));
    consumers.forEach(consumer -> consumersPool.submit(wrapRunnable(consumer)));

    Thread reportThread =
        new Thread(
            wrapRunnable(
                () -> report(arguments.statusUpdateMs)));

    reportThread.setDaemon(true);
    reportThread.setName("reporter");
    reportThread.start();

    producersPool.shutdown();
    consumersPool.shutdown();

    final long startTime = System.currentTimeMillis();
    producersPool.awaitTermination(arguments.duration, TimeUnit.MINUTES);
    final long producerEndTime = System.currentTimeMillis();

    // There could be situations where consumers finish before producers, or vice-versa, so
    // we need to make sure we wait on both executors, not just one of them
    if (producerEndTime - startTime < Duration.ofMinutes(arguments.duration).toMillis()) {
      consumersPool.awaitTermination(producerEndTime - startTime, TimeUnit.MILLISECONDS);
    }

    // arguments.statusUpdateMs

    log.info("Consumers completed: {}", consumersPool.isTerminated());
    log.info("Producers completed: {}", producersPool.isTerminated());

    log.info("=======CumulativeStats=======");
      Histogram reportHistogram = null;
      reportHistogram = Producer.cumulativeRecorder.getIntervalHistogram(reportHistogram);
      printStats(System.nanoTime() - startTsNs, reportHistogram);
  }


  private static void printStats(long durNs, Histogram histogram) {
      log.info(
              "mean: {} ms - med: {} - 95pct: {} - 99pct: {} - Max: {} - thrpt {}",
              histogram.getMean() / 1000.0,
              histogram.getValueAtPercentile(50) / 1000.0,
              histogram.getValueAtPercentile(95) / 1000.0,
              histogram.getValueAtPercentile(99) / 1000.0,
              histogram.getMaxValue() / 1000.0,
              histogram.getTotalCount() / NANOSECONDS.toSeconds(durNs));
  }

  private static void report(long updateInterval) {
    Histogram reportHistogram = null;
    Recorder intervalReporter = Producer.recorder;
    long prevTs = System.nanoTime();

    while (true) {
      try {

        Thread.sleep(updateInterval);

        reportHistogram = intervalReporter.getIntervalHistogram(reportHistogram);
        long currTs = System.nanoTime();
        printStats(currTs - prevTs, reportHistogram);
        reportHistogram.reset();
        prevTs = currTs;

      } catch (Exception e) {
        log.error("reporter failed!", e);
      }
    }
  }
}
