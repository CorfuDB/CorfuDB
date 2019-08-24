package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;
import org.corfudb.util.MetricsUtils;

@Slf4j
public class IOLatencyDetector {
    long thresh;

    //Will change to metrics later
    TraceInterval normTr;
    TraceInterval spikeTr;
    long start;

    static boolean spikeDetected = false;

    public static boolean reportSpike() {
        return spikeDetected;
    }

    IOLatencyDetector(long thresh) {
        this.thresh = thresh;
        normTr = new TraceInterval ("normal");
        spikeTr = new TraceInterval ("spiketr");
    }

    //time for each operation is l + size/average4k
    void start () {
        start = System.nanoTime ();
    }


    int size2page(int size) {
        return size >> 12;
    }

    boolean update(int size) {
        int n4k = size2page(size);
        long curr = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        long est = estimate (n4k);
        boolean triggerReport = false;

        if (curr > est * thresh) {
            spikeTr.update_sum(n4k, curr);
            if (reportSpike()) {
                //do something
                spikeDetected = true;
                log.info ("detected failure");
                return false;
            }
        } else {
            spikeDetected = false;
            spikeTr.reset();
            normTr.update_sum(n4k, curr);
        }
        return true;
    }

    long estimate(int n4k) {
        return normTr.getAverage()*n4k;
    }
}
