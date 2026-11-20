package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Long.max;

@NoArgsConstructor
public class VersionedObjectStats {

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was cached.
    private volatile long cacheEntryTs = 0;

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was last served.
    private volatile long lastAccessedTs = 0;

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was evicted.
    private volatile long evictedTs = 0;

    @Getter
    // Number of times the corresponding versioned object was served while cached.
    private final AtomicInteger numAccessCached = new AtomicInteger(0);

    public void requestedWhileCached() {
        numAccessCached.incrementAndGet();
    }

    @Override
    public String toString() {
        return String.format("%d, %d, %d, %d", cacheEntryTs, lastAccessedTs, evictedTs, numAccessCached.get());
    }

    public void recordMetrics(String streamId) {
        if (numAccessCached.get() > 0) {
            MicroMeterUtils.time(Duration.ofNanos(lastAccessedTs - cacheEntryTs),
                    "mvo.cache.used.time", "streamId", streamId);
        }
        MicroMeterUtils.time(Duration.ofNanos(evictedTs - max(lastAccessedTs, cacheEntryTs)),
                "mvo.cache.wasted.time", "streamId", streamId);
        MicroMeterUtils.measure(numAccessCached.get(), "mvo.cache.num.accesses", "streamId", streamId);
    }
}
