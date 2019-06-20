package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import org.corfudb.runtime.exceptions.QuotaExceededException;


/**
 * A quota for a resource that can be modeled with a long.
 *
 * Created by Maithem on 6/20/19.
 */
@AllArgsConstructor
public class ResourceQuota {

    private final String name;
    private final long used;
    private final long limit;

    /**
     * Consumes an amount of the resource quota.
     */
    public ResourceQuota acquire(long amount) {
        return new ResourceQuota(name, used + amount, limit);
    }

    /**
     * Checks if the resource quota has been exceeded.
     */
    public void check() {
        if (used > limit) {
            String msg = String.format("Resource Exhausted[%s]: limit=%d, exceededSize=%d",
                    name, limit, used);
            throw new QuotaExceededException(msg);
        }
    }

    /**
     * Returns some amount of the resource to the quota
     */
    public ResourceQuota release(long amount) {
        return new ResourceQuota(name, Math.max(0, used - amount), limit);
    }
}
