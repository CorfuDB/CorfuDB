package org.corfudb.perf.streaming;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.HdrHistogram.Recorder;
import org.corfudb.runtime.CorfuRuntime;

@AllArgsConstructor
public abstract class Worker implements Runnable {

    /**
     * Id of this worker
     */
    protected final UUID id;

    /**
     * Runtime to use.
     */
    protected final CorfuRuntime runtime;

    /**
     * Recorder to track latency stats
     */
    protected final Recorder recorder = new Recorder(TimeUnit.HOURS.toNanos(1), 5);

    /**
     * Total number of items to produce
     */
    protected final int numItems;

}
