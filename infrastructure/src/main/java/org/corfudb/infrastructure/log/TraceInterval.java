package org.corfudb.infrastructure.log;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TraceInterval {
    String name;
    long start;
    long total; //sum of latencise for all operations in microseconds
    long cnt;   //total number of the operations.

    TraceInterval(String name) {
        this.name = name;
        total = 0;
        cnt = 0;
    }

    void update(long val) {
        if (Long.MAX_VALUE - total < val)
            rolling (val);
        if (cnt == 0)
            start = System.nanoTime ();
        total += val;
        cnt++;
    }

    void update_sum(long cnt, long val) {
        rolling(val);
        this.cnt += cnt;
        total += val;
    }

    void rolling(long val) {
        while (Long.MAX_VALUE - total < val) {
            total = total/2;
            cnt = cnt/2;
        }
    }

    void reset() {
        start = 0;
        total = 0;
        cnt = 0;
    }

    long getAverage() {
        return total/cnt;
    }

    void log(boolean infoMode, String uname) {
        if (infoMode == true)
            log.info("{} cnt {} total {} %s average {} %s", name, cnt, total, uname, total/(1.0*cnt), uname);
        else
            log.warn("{} reset cnt {} total {} %s average {} %s", name, cnt, total, uname, total/(1.0*cnt), uname);
    }

    static void log(TraceInterval[] traces, String name, String uname) {
        long totalElapse = 0;
        long totalCnt = 0;
        for (int i = 0; i < traces.length; i++) {
            totalElapse += traces[i].total;
            totalCnt += traces[i].cnt;
        }
        log.info ("%s aggregate cnt {} total {} %s  average {} %s",
                name, totalCnt, totalElapse, uname, totalElapse/(1.0*totalCnt), uname);
    }
}
