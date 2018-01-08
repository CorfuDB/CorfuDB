package org.corfudb.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

/** Utils for sleeping with or without interruptions using timeunits, much like
 *  {@link TimeUnit}'s sleep functionality.
 *
 *  Use these methods rather than {@link Thread#sleep(long)} or {@link TimeUnit#sleep(long)}
 *  because they handle {@linbk InterruptedException} properly.
 */
public enum Sleep {
    NANOSECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.NANOSECONDS.toMillis(duration);
        }

        @Override
        int excessNanos(long duration, long milliseconds) {
            return (int)(duration - (milliseconds * (1000L * 1000L)));
        }
    },
    MICROSECONDS {
        @Override
        long toMillis(long duration) {
            return TimeUnit.MICROSECONDS.toMillis(duration);
        }

        @Override
        int excessNanos(long duration, long milliseconds) {
            return (int)((duration * 1000L) - (milliseconds * (1000L * 1000L)));
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


    /** Convert this duration to the excess nanoseconds parameter used for sleep. Only
     *  applicable for units less than a millisecond.
     * @param duration      The duration to convert.
     * @param milliseconds  The amount of milliseconds.
     * @return              The number of nanoseconds leftover by subtracting milliseconds
     *                      from duration.
     */
    int excessNanos(long duration, long milliseconds) {
        return 0;
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
    public static void sleepUninterruptibly(Duration duration) {
        final long milliseconds = duration.toMillis();
        if (milliseconds == 0) {
            NANOSECONDS.sleepUninterruptibly(duration.toNanos());
        } else {
            MILLISECONDS.sleepUninterruptibly(duration.toMillis());
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
    public void sleepUninterruptibly(long duration) {
        try {
            final long ms = toMillis(duration);
            Thread.sleep(ms, excessNanos(duration, ms));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnrecoverableCorfuInterruptedError(
                "Uninterruptible sleep interrupted", ie);
        }
    }

    /**
     * Sleep, throwing an {@link InterruptedException}
     * if the sleep is interrupted, re-setting the interrupted flag. If you call this method,
     * you should cleanup state and exit the method you are currently running in, returning to
     * the caller, by rethrowing the exception.
     *
     * @param duration      The duration to sleep.
     */
    @SuppressWarnings("checkstyle:ThreadSleep")
    public void sleepRecoverably(long duration)
        throws InterruptedException {
        try {
            final long ms = toMillis(duration);
            Thread.sleep(ms, excessNanos(duration, ms));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        }
    }
}
