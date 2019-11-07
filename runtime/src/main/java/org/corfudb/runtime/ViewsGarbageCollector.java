package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * A CorfuRuntime garbage collector. The runtime contains some views that track
 * the global log. As the compaction mark get updated, the runtime's views need
 * to remove log metadata that corresponds to the trimmed parts of the log.
 * <p>
 * Created by Maithem on 11/16/18.
 */

@Slf4j
@NotThreadSafe
public class ViewsGarbageCollector {

    final ExecutorService gcThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("ViewsGarbageCollector")
                    .build());

    final CorfuRuntime runtime;

    public ViewsGarbageCollector(CorfuRuntime runtime) {
        this.runtime = runtime;
    }


    public void stop() {
        gcThread.shutdownNow();
    }

    public void gc(long trimMark) {
        gcThread.execute(() -> runRuntimeGC(trimMark));
    }

    /**
     * Over time garbage is created on the global log and clients sync
     * parts of the log therefore clients can accumulate garbage overtime. This
     * method runs a garbage collection process on the different client views,
     * which discards data before the trim mark.
     */
    @VisibleForTesting
    public void runRuntimeGC(long trimMark) {
        try {
            log.info("runRuntimeGC: starting gc cycle, attempting to remove to {}", trimMark);
            long startTs = System.currentTimeMillis();

            runtime.getObjectsView().gc(trimMark);
            runtime.getStreamsView().gc(trimMark);
            runtime.getAddressSpaceView().gc(trimMark);
            long endTs = System.currentTimeMillis();
            log.info("runRuntimeGC: completed gc in {}ms on {} object(s) at trimMark {}",
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
