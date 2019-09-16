package org.corfudb.infrastructure;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.IOLatencyDetector;
import org.corfudb.util.MetricsUtils;

import java.util.IdentityHashMap;
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

            System.out.println ("start metrics size " + MetricsUtils.sizeOf.deepSizeOf(metrics));

            for (long i = 0; i < numFeeds; i++) {
                try {
                    //int val = random.nextInt (10000);
                    //System.out.println ("threadname :" + threadName + " add val: " + val);
                    long start = System.nanoTime ();
                    val = 10;
                    if (i % 100 > 95)
                        val = 2000;
                    sleep (val);
                    IOLatencyDetector.update (metrics, threadName, start, 1);
                } catch (Exception e) {
                    // ignore exception
                    log.warn ("statusReporter: encountered exception", e);
                }
            }

            System.out.println ("end metrics size " + MetricsUtils.sizeOf.deepSizeOf(metrics));
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

        public MetricsPoller(String name, long numPolls) {
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
                    IOLatencyDetector.metricsHis ();

                } catch (Exception e) {
                    // ignore exception
                    log.warn ("statusReporter: encountered exception", e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        IOLatencyDetector.setupIOLatencyDetector (10, 1);

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

        IdentityHashMap<Object, Object> visited = new IdentityHashMap<Object, Object>();
        for (int i = 0; i < 1000000; i++) {
            visited.put (new Integer(i), new Integer(i));
        }

        System.out.println ("size " + MetricsUtils.sizeOf.deepSizeOf(visited));

        //create a thread to feed read latency
        Timer readMetrics = IOLatencyDetector.getReadMetrics();
        MetricsFeeder r1 = new MetricsFeeder(readMetrics.toString (), readMetrics, numFeeds);
        r1.start();

        //create a thread to feed write latency
        Timer writeMetrics = IOLatencyDetector.getWriteMetrics();
        MetricsFeeder r2 = new MetricsFeeder(writeMetrics.toString (), writeMetrics, numFeeds);
        r2.start();

        //create a thread to poll detection
        MetricsPoller poller = new MetricsPoller ("testpoller", numPolls);
        poller.start ();
    }
}
