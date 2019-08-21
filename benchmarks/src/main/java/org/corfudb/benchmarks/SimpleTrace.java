package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleTrace {
    String name;
    long total; //in microseconds
    long cnt;
    long start; //in nanoseconds

    SimpleTrace(String s) {
        name = s;
        total = 0;
        cnt = 0;
        start = System.nanoTime();
    }

    void start() {
        start = System.nanoTime();
    }

    void end() {
        long interval = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        //avoid to overflow the value, do a reset.
        if (Long.MAX_VALUE - total < interval) {
            this.log(false);
            cnt = cnt/2;
            total = total/2;
        }

        total += interval;
        cnt++;
    }

    void log(boolean info) {
        if (info == true)
            log.info("{} cnt {} total {} ms  average {} micros", name, cnt, total/1000, total/(1.0*cnt));
        else
            log.warn("{} reset cnt {} total {} ms  average {} micros", name, cnt, total/1000, total/(1.0*cnt));
    }

    static void log(SimpleTrace[] traces, String s) {
        long total_elapse = 0;
        long total_cnt = 0;
        for (int i = 0; i < traces.length; i++) {
            total_elapse += traces[i].total;
            total_cnt += traces[i].cnt;
        }
        log.info ("{} aggregate cnt {} total {} ms  average {} micros",
                s, total_cnt, total_elapse/1000, total_elapse/(1.0*total_cnt));
    }
}
