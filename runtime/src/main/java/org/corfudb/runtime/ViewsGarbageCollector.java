package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A CorfuRuntime garbage collector. The runtime contains some views that track
 * the global log, as sections of the global log get checkpointed and trimmed
 * the runtime's views need to remove log metadata that corresponds to the trimmed
 * parts of the log.
 * <p>
 * Created by Maithem on 11/16/18.
 */

@Slf4j
@NotThreadSafe
public class ViewsGarbageCollector {


    private long trimMark = Address.NON_ADDRESS;

    @Getter
    private boolean started;

    final ScheduledExecutorService gcThread = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("ViewsGarbageCollector")
                    .build());

    final CorfuRuntime runtime;

    public ViewsGarbageCollector(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.started = false;
    }

    public void start() {
        gcThread.scheduleAtFixedRate(() -> runRuntimeGC(),
                runtime.getParameters().getRuntimeGCPeriod().toMinutes(),
                runtime.getParameters().getRuntimeGCPeriod().toMinutes(),
                TimeUnit.MINUTES);
        this.started = true;
    }

    public void stop() {
        gcThread.shutdownNow();
        this.started = false;
    }

    /**
     * Over time garbage is created on the global log and clients sync
     * parts of the log therefore clients can accumulate garbage overtime. This
     * method runs a garbage collection process on the different client views,
     * which discards data before the trim mark (i.e. the point that demarcates
     * the beginning of the active log).
     */
    public void runRuntimeGC() {
        try {
            // Trim mark reflects the first untrimmed address, therefore, GC is performed up until trim mark (not included).
            long currTrimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();
            log.info("runRuntimeGC: starting gc cycle, attempting to remove {} to {}", trimMark,
                    currTrimMark);
            long startTs = System.currentTimeMillis();

            // Note: the stream layer will defer GC on this trimMark for the next cycle.
            // This is done to avoid data loss whenever the current context is at a version in the trim area. If we GC
            // right away we would need to abort transactions in the trim range and additionally reset the stream
            // so transactions at versions over the trim mark can recover their state.
            // To avoid this, a flag will be set so we start aborting ongoing transactions in this trimmed area and
            // let the next GC cycle discard the data.
            runtime.getObjectsView().gc(currTrimMark);
            runtime.getStreamsView().gc(currTrimMark);
            runtime.getAddressSpaceView().gc(currTrimMark);
            long endTs = System.currentTimeMillis();
            trimMark = currTrimMark;
            log.info("runRuntimeGC: completed gc in {}ms on {} object(s), new trimMark {}",
                    endTs - startTs, runtime.getObjectsView().getObjectCache().size(), trimMark);
        } catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                throw new UnrecoverableCorfuInterruptedError((InterruptedException) e.getCause());
            } else {
                log.error("Encountered an error while running runtime GC", e);
            }
        }
    }
}
