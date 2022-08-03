package org.corfudb.runtime.object;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class MVOCacheEviction {

    @Getter
    private static final long DEFAULT_EVICTION_INTERVAL_IN_MILLISECONDS = 100;

    public static final AtomicInteger threadCount = new AtomicInteger(0);

    private final String threadName;
    private final ScheduledExecutorService evictionThread;

    private final ConcurrentLinkedQueue<VersionedObjectIdentifier> versionsToEvict = new ConcurrentLinkedQueue<>();

    private final EvictFunction evictFunc;

    public MVOCacheEviction(EvictFunction evictFunc) {
        this.evictFunc = evictFunc;
        this.threadName = "MVOCacheEvictionThread-" + threadCount;
        MVOCacheEviction.threadCount.getAndIncrement();

        evictionThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(true)
                        .setNameFormat(threadName)
                        .build());
    }

    public void start() {
        evictionThread.scheduleAtFixedRate(this::evict,
                DEFAULT_EVICTION_INTERVAL_IN_MILLISECONDS,
                DEFAULT_EVICTION_INTERVAL_IN_MILLISECONDS,
                TimeUnit.MILLISECONDS);
    }

    public void evict() {
        versionsToEvict.stream()
            .collect(Collectors.groupingBy(VersionedObjectIdentifier::getObjectId))
            .forEach((uuid, voIds) -> {
                Preconditions.checkState(!voIds.isEmpty());
                Long maxVersion = voIds.stream()
                        .map(VersionedObjectIdentifier::getVersion)
                        .max(Long::compare).get();
                int count = evictFunc.evict(new VersionedObjectIdentifier(uuid, maxVersion));
                log.info("evicted {} versions for object {} up to version {} (inclusive).",
                        count, uuid, maxVersion);
            });
        versionsToEvict.clear();
    }

    public void add(VersionedObjectIdentifier voId) {
        log.info("MVOCacheEviction add eviction task {}", voId);
        if (voId.getVersion() < 0) {
            return;
        }
        versionsToEvict.add(voId);
    }

    public void shutdown() {
        evictionThread.shutdownNow();
        log.info("{} MVO Cache eviction thread shut down", threadName);
    }
}

@FunctionalInterface
interface EvictFunction {
    int evict(VersionedObjectIdentifier voId);
}
