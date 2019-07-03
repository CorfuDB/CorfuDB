package org.corfudb.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

/** Utils for sleeping with or without interruptions using timeunits, much like
 *  {@link TimeUnit}'s sleep functionality.
 *
 *  Use these methods rather than {@link Thread#sleep(long)} or {@link TimeUnit#sleep(long)}
 *  because they handle {@linbk InterruptedException} properly.
 *
 *  @deprecated Sleep doesn't have any advantages over TimeUnit. Please always use TimeUnit.
 */
@Deprecated
public enum Sleep {
    NANOSECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.NANOSECONDS.toMillis(duration);
        }
    },
    MICROSECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.MICROSECONDS.toMillis(duration);
        }
    },
    MILLISECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.MILLISECONDS.toMillis(duration);
        }
    },
    SECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.SECONDS.toMillis(duration);
        }
    },
    MINUTES {
        @Override
        long toMillis(long duration) {
            return TimeUnit.MINUTES.toMillis(duration);
        }
    },
    HOURS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.HOURS.toMillis(duration);
        }
    },
    DAYS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.DAYS.toMillis(duration);
        }
    };

    /** Convert this duration to millis. Used for the millisecond parameter to sleep.
     *
     * @param duration  The duration to convert.
     * @return          The duration in milliseconds.
     */
    abstract long toMillis(long duration);

    /**
     * Sleep, without recovery logic in case the sleep is interrupted.
     *
     * <p>If sleep is interrupted, this method will throw
     * {@link UnrecoverableCorfuInterruptedError}, which will indicate that Corfu was interrupted
     * but cannot recover from the interruption.
     *
     * @param duration      The duration to sleep.
     */
    public static void sleepUninterruptibly(Duration duration) {
        final long milliseconds = duration.toMillis();
        if (milliseconds == 0) {
            sleepUninterruptibly(duration.toNanos(), TimeUnit.NANOSECONDS);
        } else {
            sleepUninterruptibly(duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sleep, without recovery logic in case the sleep is interrupted.
     *
     * <p>If sleep is interrupted, this method will throw
     * {@link UnrecoverableCorfuInterruptedError}, which will indicate that Corfu was interrupted
     * but cannot recover from the interruption.
     *
     * @param duration      The duration to sleep.
     */
    @SuppressWarnings("checkstyle:ThreadSleep")
    private static void sleepUninterruptibly(long duration, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(duration);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError("Uninterruptible sleep interrupted", ie);
        }
    }
}
