package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class StatusReporter extends Thread {
    private LongAdder requestsCompleted = null;
    private Recorder recorder = null;

    public StatusReporter(LongAdder requestsCompleted, Recorder recorder) {
        this.requestsCompleted = requestsCompleted;
        this.recorder = recorder;
    }

    @SuppressWarnings("checkstyle:printLine")
    @Override
    public void run() {
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

                System.out.printf("Throughput %f req/sec   Latency{ total{%f}ms  mean{%f}ms fiftyPercent{%f}ms NintyFivePercent{%f}ms NintyNinePercent{%f}ms\n",
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
                //System.out.println("statusReporter: encountered exception" + e);
            }
        }
    }
}