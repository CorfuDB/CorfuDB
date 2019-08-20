package org.corfudb.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;


/**
 * Created by Maithem on 8/1/19.
 */
@Slf4j
public class SequencerFlood {
    static class Args {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--endpoint"}, description = "Cluster endpoint", required = true)
        String endpoint; //ip:portnum

        @Parameter(names = {"--num-clients"}, description = "Number of clients", required = true)
        int numClients;

        @Parameter(names = {"--num-threads"}, description = "Total number of threads", required = true)
        int numThreads;

        @Parameter(names = {"--num-requests"}, description = "Number of requests per thread", required = true)
        int numRequests;
    }

    public static void main(String[] args) throws Exception {

        Args cmdArgs = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdArgs)
                .build();
        jc.parse(args);

        if (cmdArgs.help) {
            jc.usage();
            System.exit(0);
        }

        int numRuntimes = cmdArgs.numClients;
        int numThreads = cmdArgs.numThreads;
        int numRequests = cmdArgs.numRequests;
        CorfuRuntime[] rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(cmdArgs.endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);

        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        LongAdder requestsCompleted = new LongAdder();
        Recorder recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        SimpleTrace[] traces = new SimpleTrace[numThreads];

        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];
            int id = tNum;
            service.submit(() -> {
                traces[id] = new SimpleTrace ("Recorder");
                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().query();
                    long end = System.nanoTime();
                    traces[id].start();
                    recorder.recordValue(TimeUnit.NANOSECONDS.toMicros(end - start));
                    traces[id].end();
                    requestsCompleted.increment();
                }
            });
        }

        Thread statusReporter = new Thread(() -> {
            long currTs;
            long prevTs = 0;
            long currMsgCnt;
            long prevMsgCnt = 0;
            while (true) {
                try {
                    currTs = System.currentTimeMillis ();
                    currMsgCnt = requestsCompleted.intValue ();
                    double throughput = (currMsgCnt - prevMsgCnt) / ((currTs - prevTs) / 1000);
                    Histogram histogram = recorder.getIntervalHistogram ();

                    log.info ("Throughput {} req/sec Latency: total: {} ms mean: {} ms  50%: {} ms  95%: {} ms  99%: {} ms",
                            throughput,
                            histogram.getTotalCount () / 1000.0,
                            histogram.getMean () / 1000.0,
                            histogram.getValueAtPercentile (50) / 1000.0,
                            histogram.getValueAtPercentile (95) / 1000.0,
                            histogram.getValueAtPercentile (99) / 1000.0);

                    Sleep.sleepUninterruptibly (Duration.ofMillis (1000 * 3));
                    prevTs = currTs;
                    prevMsgCnt = currMsgCnt;
                } catch (Exception e) {
                    // ignore exception
                    log.warn ("statusReporter: encountered exception", e);
                }
            }
        });

        statusReporter.setDaemon(true);
        statusReporter.start();

        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        SimpleTrace.log(traces, "Recorder Overhead");
    }
}
