package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@NotThreadSafe
public class GarbageInformer {

    private volatile boolean paused = false;

    @Getter
    private BlockingQueue<GarbageMark> garbageMarkQueue = new LinkedBlockingQueue<>();

    final ExecutorService garbageInformerService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("GarbageInformation-Processor-%d")
                    .build());

    final CorfuRuntime runtime;

    public GarbageInformer(@NonNull CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    public void start() {
        paused = false;
        garbageInformerService.submit(this::garbageInformationProcessor);
    }

    /**
     * Gracefully shutdown the service.
     */
    @VisibleForTesting
    public void stop() throws InterruptedException {
        if (paused) {
            log.warn("Try to shutdown garbage informer service when it is paused.");
        }

        garbageMarkQueue.add(GarbageMark.SHUTDOWN);
        garbageInformerService.shutdown();
        garbageInformerService.awaitTermination(runtime.getParameters().GC_SHUTDOWN_TIMER.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void pause() {
        if (paused) {
            log.warn("Try to pause garbage informer service when it is paused.");
        }
        paused = true;
    }

    public void resume() {
        if (!paused) {
            log.warn("Try to resume garbage informer service when it is not paused.");
        }
        paused = false;
    }

    public void prefixTrim(UUID streamId, ISMREntryLocator locator) {
        try{
            CompletableFuture<Void> cf = prefixTrimFuture(streamId, locator);
            cf.get();
        } catch (Exception ex) {
            log.trace("Garbage PrefixTrim Exception: {}", ex);
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    public CompletableFuture<Void> prefixTrimFuture(UUID streamId, ISMREntryLocator locator) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        garbageMarkQueue.add(new GarbageMark(GarbageMark.GarbageMarkType.PREFIXTRIM,
                streamId, locator, cf));
        return cf;
    }

    public void sparseTrim(UUID streamId, ISMREntryLocator locator) {
        try {
            CompletableFuture cf = sparseTrimFuture(streamId, locator);
            cf.get();
        } catch (Exception ex) {
            log.trace("Garbage SparseTrim Exception: {}", ex);
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }

    }

    public CompletableFuture<Void> sparseTrimFuture(UUID streamId, ISMREntryLocator locator) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        garbageMarkQueue.add(new GarbageMark(GarbageMark.GarbageMarkType.SPARSETRIM, streamId, locator, cf));
        return cf;
    }

    //TODO(Xin): Current the asynchronous processor is no-op
    private void garbageInformationProcessor() {
        try {
            while (true) {
                if(!paused) {
                    GarbageMark garbageMark = garbageMarkQueue.take();

                    if (garbageMark.getType().equals(GarbageMark.GarbageMarkType.SHUTDOWN)) {
                        break;
                    } else {
                        garbageMark.getCf().complete(null);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Caught exception in the write processor ", e);
        }
    }
}
