package org.corfudb.infrastructure;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A quota for a resource that can be modeled with a long.
 *
 * Created by Maithem on 6/20/19.
 */
public class ResourceQuota {

    @Getter
    private final String name;

    @Getter
    private final long limit;

    private final AtomicLong used = new AtomicLong();

    public ResourceQuota(String name, long limit) {
        this.name = name;
        if (limit <= 0) {
            throw new IllegalArgumentException("Resource limit has to be positive: " + limit);
        }
        this.limit = limit;
    }

    /**
     * Consumes an amount of the resource quota.
     */
    public void consume(long amount) {
        used.updateAndGet(currVal -> Math.min(Math.addExact(currVal, amount), Long.MAX_VALUE));
    }

    /**
     * Checks if the resource quota has been exceeded.
     */
    public boolean hasAvailable() {
        return used.get() < limit;
    }

    /**
     * Returns some amount of the resource to the quota
     */
    public void release(long amount) {
        used.updateAndGet(currVal -> Math.max(0, Math.subtractExact(currVal, amount)));
    }

    /**
     * Returns the amount of available resources
     */
    public long getAvailable() {
        return Math.max(limit - used.get(), 0);
    }

}
