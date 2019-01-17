package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.GarbageCollectorException;
import org.corfudb.runtime.view.Address;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Periodic tasks to maintain the CorfuRuntime Views. The runtime contains some views that track
 * the global log, as sections of the global log get checkpointed and trimmed
 * the runtime's views need to remove log metadata that corresponds to the trimmed
 * parts of the log. Also, auto sync objects to amortize the log sync costs.
 * <p>
 * Created by Maithem on 11/16/18.
 */

@Slf4j
@NotThreadSafe
public class ViewsMaintenance {


    private long trimMark = Address.NON_ADDRESS;

    @Getter
    private boolean started;

    /**
     * How often to sync opened objects
     */
    @Getter
    @Setter
    long objectSyncDelayInSeconds = 30;

    /**
     * How often to run GC on the opened objects
     */
    @Getter
    @Setter
    long objectGCPeriodInSeconds = 20 * 60;

    /**
     * This single thread is shared by the two tasks (i.e. object GC and object syncing)
     */
    final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("ViewsMaintenance")
                    .build());

    final CorfuRuntime runtime;

    public ViewsMaintenance(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.started = false;
    }

    public void start() {
        maintenance.scheduleAtFixedRate(() -> runRuntimeGC(),
                objectGCPeriodInSeconds, objectGCPeriodInSeconds,
                TimeUnit.SECONDS);
        maintenance.scheduleAtFixedRate(() -> runSyncObjects(),
                objectSyncDelayInSeconds, objectGCPeriodInSeconds,
                TimeUnit.SECONDS);
        this.started = true;
    }

    public void stop() {
        maintenance.shutdownNow();
        this.started = false;
    }

    /**
     * Objects are synced on access, if a client access an object irregularly, then over time
     * it can experience unpredictable latencies because of syncing. When used, the ViewsMaintinance
     * will periodically sync the object ao amortize the sync costs.
     */
    private void runSyncObjects() {
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
