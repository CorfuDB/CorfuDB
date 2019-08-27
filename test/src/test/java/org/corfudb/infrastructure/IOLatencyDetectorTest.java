package org.corfudb.infrastructure;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.IOLatencyDetector;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import static java.lang.Thread.sleep;

@Slf4j
public class IOLatencyDetectorTest {
    static class Args {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--num-feeds"}, description = "Number of feeds", required = true)
        int numFeeds;

        @Parameter(names = {"--num-polls"}, description = "Number of polls", required = true)
        int numPolls;
    }

    static class MetricsFeeder implements Runnable {
        private Thread t;
        private String threadName;
        private Timer metrics;
        long numFeeds;

        MetricsFeeder(String name, Timer metrics, long numFeeds) {
            this.threadName = name + "metrics for " + metrics.toString ();
            this.metrics = metrics;
            this.numFeeds = numFeeds;

            System.out.println("Creating " +  threadName );
        }

        public void run() {
            System.out.println ("Running " + threadName);
            Random random = new Random ();
            int val;

            for (long i = 0; i < numFeeds; i++) {
                try {
                    //int val = random.nextInt (10000);
                    //System.out.println ("threadname :" + threadName + " add val: " + val);
                    long start = System.nanoTime ();
                    val = 10;
                    if (i % 100 > 95)
                        val = 2000;
                    sleep (val);
                    IOLatencyDetector.update (metrics, start, 1);
                } catch (Exception e) {
                    // ignore exception
                    log.warn ("statusReporter: encountered exception", e);
                }
            }
        }

        public void start () {
            System.out.println("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }
    }

    static class MetricsPoller implements Runnable {
        private Thread t;
        private String threadName;
        private IOLatencyDetector ioLatencyDetector;
        private long numPolls;

        public MetricsPoller(String name, IOLatencyDetector ioLatencyDetector, long numPolls) {
            this.threadName = name;
            this.ioLatencyDetector = ioLatencyDetector;
            this.numPolls = numPolls;
        }

        public void start () {
            System.out.println("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.run ();
            }
        }

        @Override
        public void run() {
            log.info("starting run");
            System.out.println("Running " +  threadName + " " + numPolls);
            for(long i = 0; i < numPolls; i++) {
                try {
                    sleep (1000);
                    if (IOLatencyDetector.reportSpike ()) {
                        System.out.println ("------spike detected------");
                    }
                    IOLatencyDetector.metricsHis (ioLatencyDetector);

                } catch (Exception e) {
                    // ignore exception
                    log.warn ("statusReporter: encountered exception", e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        IOLatencyDetector ioLatencyDetector = new IOLatencyDetector (10, 1,
                "WriteTest", "ReadTest");

        Args cmdArgs = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdArgs)
                .build();
        jc.parse(args);

        if (cmdArgs.help) {
            jc.usage();
            System.exit(0);
        }

        long numFeeds = cmdArgs.numFeeds;
        long numPolls = cmdArgs.numPolls;

        //create a thread to feed read latency
        Timer readMetrics = ioLatencyDetector.getReadMetrics();
        MetricsFeeder r1 = new MetricsFeeder(readMetrics.toString (), readMetrics, numFeeds);
        r1.start();

        //create a thread to feed write latency
        Timer writeMetrics = ioLatencyDetector.getWriteMetrics();
        MetricsFeeder r2 = new MetricsFeeder(writeMetrics.toString (), writeMetrics, numFeeds);
        r2.start();

        //create a thread to poll detection
        MetricsPoller poller = new MetricsPoller ("testpoller", ioLatencyDetector, numPolls);
        poller.start ();
    }
}
