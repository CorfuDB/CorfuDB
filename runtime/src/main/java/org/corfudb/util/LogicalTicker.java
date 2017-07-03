package org.corfudb.util;

import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;

import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.protocols.wireprotocol.ILogData;

/**
 * A Ticker subclass for manipulating Caffeine cache expiry
 * policy using logical time.
 */

public class LogicalTicker implements Ticker {
    AtomicLong time;

    // Caffeine's algorithm assumes fairly "wide" distances between
    // times, e.g. nanoseconds, see commentary on GitHub PR 801.
    private static final long scaleFactor = 1_000_000_000;

    public LogicalTicker(AtomicLong time) {
        this.time = time;
    }

    @Override
    public long read() {
        long time = this.time.get();
        if (time < 0) {
            return 0; // Don't report a negative clock
        } else {
            return time * scaleFactor;
        }
    }

    /** Return an expiry policy based on logical time. */
    public static Expiry<Long, ILogData> caffeineLogicalExpiryPolicy() {
        // Use a policy of logical time, where the logical clock advances
        // each time that we witness a TrimmedException.  The "time" for
        // a cache entry's expiry is its log address.

        return new Expiry<Long, ILogData>() {
            public long expireAfterCreate(Long key, ILogData l, long currentTime) {
                return (key * scaleFactor) - currentTime;
            }

            public long expireAfterUpdate(Long key, ILogData l,
                                          long currentTime, long currentDuration) {
                return (key * scaleFactor) - currentTime;
            }

            public long expireAfterRead(Long key, ILogData l,
                                        long currentTime, long currentDuration) {
                return (key * scaleFactor) - currentTime;
            }
        };
    }
}
