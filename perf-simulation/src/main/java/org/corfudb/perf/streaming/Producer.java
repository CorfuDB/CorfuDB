package org.corfudb.perf.streaming;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

@Slf4j
public class Producer extends Worker {

    public static Recorder recorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    public static Recorder cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    /**
     * The payload to write to the stream on each "produce"
     */
    private final byte[] payload;

    public Producer(final UUID id, final CorfuRuntime runtime,
                    final int numItems, final byte[] payload) {
        super(id, runtime, numItems);
        this.payload = payload;
    }


    @Override
    public void run() {
        log.debug("Producer[{}] started", id);
        final long startTime = System.currentTimeMillis();
        final IStreamView stream = runtime.getStreamsView().get(id);

        for (int taskNum = 0; taskNum < numItems; taskNum++) {
            final long startTimestamp = System.nanoTime();
            stream.append(payload);
            final long appendDuration = System.nanoTime() - startTimestamp;
            recorder.recordValue(NANOSECONDS.toMicros(appendDuration));
            //cumulativeRecorder.recordValue(appendDuration);
        }

        final double totalTimeInSeconds =  (System.currentTimeMillis() - startTime * 1.0) / 10e3;
        log.debug("Producer[{}] completed {} in {} seconds", id, numItems, totalTimeInSeconds);
    }
}
