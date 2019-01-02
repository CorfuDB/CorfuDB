package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.GarbageCollectorException;
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

    final ScheduledExecutorService syncThread = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("ViewsGarbageCollector-sync")
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
        //1200
        syncThread.scheduleAtFixedRate(() -> syncObjects(),
                10, 30, TimeUnit.SECONDS);
        this.started = true;
    }

    public void stop() {
        gcThread.shutdownNow();
        syncThread.shutdownNow();
        this.started = false;
    }

    private void syncObjects() {
        try {
            long s1 = System.currentTimeMillis();
            runtime.getObjectsView().syncObjects();
            long s2 = System.currentTimeMillis();
            log.info("syncObjects: sync took {} ms, total maps {}", s2 - s1, runtime.getObjectsView()
                    .getObjectCache().size());
        } catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new GarbageCollectorException(e);
            } else {
                log.error("Encountered an error while running runtime GC", e);
            }
        }
    }

    /**
     * Over time garbage is created on the global log and clients clients sync
     * parts of the log therefore clients can accumulate garbage overtime. This
     * method runs a garbage collection process on the different client views,
     * which discards data before the trim mark (i.e. the point that demarcates
     * the beginning of the active log).
     */
    public void runRuntimeGC() {
        try {
            long currTrimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();
            if (trimMark == currTrimMark) {
                log.info("runRuntimeGC: TrimMark({}) hasn't changed, skipping.");
                return;
            }

            log.info("runRuntimeGC: starting gc cycle, removing {} to {}", trimMark,
                    currTrimMark);
            long startTs = System.currentTimeMillis();
            runtime.getObjectsView().gc(currTrimMark);
            runtime.getStreamsView().gc(currTrimMark);
            runtime.getAddressSpaceView().gc(currTrimMark);
            long endTs = System.currentTimeMillis();
            trimMark = currTrimMark;
            log.info("runRuntimeGC: completed gc in {}ms on {} object(s), new trimMark {}",
                    endTs - startTs, runtime.getObjectsView().getObjectCache().size(), trimMark);
        } catch (Exception e) {
            if (e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new GarbageCollectorException(e);
            } else {
                log.error("Encountered an error while running runtime GC", e);
            }
        }
    }
}
