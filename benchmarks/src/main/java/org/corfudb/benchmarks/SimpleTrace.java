package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleTrace {
    String name; //name of the tracing object
    long total; //sum of latencise for all operations in microseconds
    long cnt;   //total number of the operations.
    long start; //the current operation start time in nanoseconds

    SimpleTrace(String name) {
        this.name = name;
        total = 0;
        cnt = 0;
        start = System.nanoTime();
    }

    void start() {
        start = System.nanoTime();
    }

    void end() {
        long interval = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        //avoid to overflow the value, do a reset. Keep half of the history.
        if (Long.MAX_VALUE - total < interval) {
            this.log(false);
            cnt = cnt/2;
            total = total/2;
        }

        total += interval;
        cnt++;
    }

    void log(boolean infoMode) {
        if (infoMode == true) {
            System.out.printf("{%s} cnt {%d} total {%d} ms  average {%f} micros\n", name, cnt, total/1000, total/(1.0*cnt));
            log.info("{} cnt {} total {} ms  average {} micros", name, cnt, total/1000, total/(1.0*cnt));
        }
        else {
            System.out.printf("{%s} reset cnt {%d} total {%d} ms  average {%f} micros", name, cnt, total/1000, total/(1.0*cnt));
            log.warn("{} reset cnt {} total {} ms  average {} micros", name, cnt, total/1000, total/(1.0*cnt));
        }
    }

    static void log(SimpleTrace[] traces, String name) {
        long totalElapse = 0;
        long totalCnt = 0;
        for (int i = 0; i < traces.length; i++) {
            totalElapse += traces[i].total;
            totalCnt += traces[i].cnt;
        }
        System.out.printf("{%s} aggregate cnt {%d} total {%d} ms  average {%f} micros", name, totalCnt, totalElapse/1000, totalElapse/(1.0*totalCnt));
        log.info ("{} aggregate cnt {} total {} ms  average {} micros",
                name, totalCnt, totalElapse/1000, totalElapse/(1.0*totalCnt));
    }
}
