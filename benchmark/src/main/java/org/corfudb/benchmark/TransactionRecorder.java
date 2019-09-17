package org.corfudb.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.Sleep;


import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;


@Slf4j
public class TransactionRecorder {
    public static void main(String[] args) throws Exception {
        int numRuntimes = 1;
        int numThreads = 2;
        int numRequests = 100000;
        String endpoint = "localhost:9000";
        CorfuRuntime[] rts = new CorfuRuntime[numRuntimes];

        // one runtime
        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);

        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        LongAdder requestsCompleted = new LongAdder();
        Recorder recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        SimpleTrace[] traces = new SimpleTrace[numThreads];

        for (int tNum = 0; tNum < numThreads; tNum++) {
            CorfuRuntime rt = rts[tNum % numRuntimes];
            int id = tNum;
            service.submit(() -> {
                CorfuTable map = rt.getObjectsView()
                        .build()
                        .setStreamName(String.valueOf(id))
                        .setType(CorfuTable.class)
                        .open();

                traces[id] = new SimpleTrace("Recorder");
                for (int i = 1; i <= numRequests; i++) {
                    long start = System.nanoTime();
                    rt.getObjectsView().TXBegin();
                    map.put(i, i);
                    rt.getObjectsView().TXEnd();
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
        for (int i = 0; i < numRuntimes; i++) {
            traces[i].log(true);
        }
        SimpleTrace.log(traces, "Recorder Overhead");

        for (int x = 0; x < rts.length; x++) {
            rts[x].shutdown();
        }
    }
}

