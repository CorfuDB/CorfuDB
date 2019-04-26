package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMREntryLocator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@NotThreadSafe
public class GarbageInformer {

    static final int BATCH_SIZE = 50;

    private BlockingQueue<GarbageMark> garbageMarkQueue = new LinkedBlockingQueue<>();

    private Map<UUID, ISMREntryLocator> streamToPrefixTrimMark = new HashMap<>();

    final ExecutorService informerService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("GarbageInformation-Processor-%d")
                    .build());

    final CorfuRuntime runtime;

    public GarbageInformer(@NonNull CorfuRuntime runtime) {
        this.runtime = runtime;
        informerService.submit(this::garbageInformationProcessor);
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
            CompletableFuture<Void> cf = sparseTrimFuture(streamId, locator);
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

    private void garbageInformationProcessor() {

    }
}
