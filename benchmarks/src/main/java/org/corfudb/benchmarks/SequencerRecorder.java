package org.corfudb.benchmarks;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.concurrent.TimeUnit;
@Slf4j
public class SequencerRecorder extends PerformanceTest{
    @SuppressWarnings("checkstyle:printLine")
    public SequencerRecorder(String[] args) {
        super(args);
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

        StatusReporter statusReporter = new StatusReporter(requestsCompleted, recorder);
        statusReporter.setDaemon(true);
        statusReporter.start();

        service.shutdown();
        try {
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.warn (e.toString());
        }
        SimpleTrace.log(traces, "Recorder Overhead");
    }

    public static void main(String[] args) {
        new SequencerRecorder(args);
    }
}
