package org.corfudb.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
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
        String url;

        @Parameter(names = {"--num-clients"}, description = "Number of clients", required = true)
        int numClients;

        @Parameter(names = {"--num-threads"}, description = "Number of threads", required = true)
        int numThreads;

        @Parameter(names = {"--num-requests"}, description = "Total number of requests", required = true)
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
            System.exit(-1);
        }


        int numRuntimes = cmdArgs.numClients;
        int numThreads = cmdArgs.numThreads;
        int numRequests = cmdArgs.numRequests;

        CorfuRuntime[] rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(cmdArgs.url).connect();
        }

        log.info("Connected {} runtimes...", numRuntimes);

        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        LongAdder requestsCompleted = new LongAdder();
        Recorder recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);


        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % rts.length];

            service.submit(() -> {

                for (int reqId = 0; reqId < numRequests; reqId++) {
                    long start = System.nanoTime();
                    rt.getSequencerView().query();
                    long end = System.nanoTime();
                    recorder.recordValue(TimeUnit.NANOSECONDS.toMicros(end - start));
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
                    currTs = System.currentTimeMillis();
                    currMsgCnt = requestsCompleted.intValue();
                    double throughput = (currMsgCnt - prevMsgCnt) / ((currTs - prevTs) / 1000);
                    log.info("Throughput {} req/sec", throughput);
                    Sleep.sleepUninterruptibly(Duration.ofMillis(1000 * 3));
                    prevTs = currTs;
                    prevMsgCnt = currMsgCnt;
                } catch (Exception e) {
                    // ignore exception
                    log.warn("statusReporter: encountered exception", e);
                }
            }
        });

        statusReporter.setDaemon(true);
        statusReporter.start();

        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);



    }
}
