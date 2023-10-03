package org.corfudb.runtime.object;

import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
public class VersionedObjectStats {

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was first generated.
    private volatile long generationTs = 0;

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was cached.
    private volatile long cacheEntryTs = 0;

    @Setter
    // Timestamp (nanoseconds) for which the corresponding versioned object was last served.
    private volatile long lastAccessedTs = 0;

    // Number of times the corresponding versioned object was served before being cached.
    private final AtomicInteger numAccessGenerated = new AtomicInteger(0);

    // Number of times the corresponding versioned object was served while cached.
    private final AtomicInteger numAccessCached = new AtomicInteger(0);

    public void requestedWhileGenerated() {
        numAccessGenerated.incrementAndGet();
    }

    public void requestedWhileCached() {
        numAccessCached.incrementAndGet();
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d", generationTs, cacheEntryTs, lastAccessedTs,
                numAccessGenerated.get(), numAccessCached.get());
    }
}
