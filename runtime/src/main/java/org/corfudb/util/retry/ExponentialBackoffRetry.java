package org.corfudb.util.retry;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;


/**
 * This class implements a basic exponential backoff retry.
 *
 * <p>Created by mwei on 9/1/15.
 */
@Slf4j
public class ExponentialBackoffRetry<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, O, A extends IRetry>
        extends AbstractRetry<E, F, G, H, O, ExponentialBackoffRetry> {

    private static final int DEFAULT_BASE = 2;
    private static final int DEFAULT_MULT = 2;
    private static final float DEFAULT_RANDOM_PORTION = 0f;
    private static final Duration DEFAULT_BACKOFF_DURATION = Duration.ofMinutes(1);
    private long retryCounter = 0;
    private long nextBackoffTime = 0;


    @Getter
    @Setter
    private boolean stopAfterBackoffDurationExceeded = false;

    @Getter
    @Setter
    private boolean stop = false;

    @Getter
    @Setter
    private Duration backoffDuration = DEFAULT_BACKOFF_DURATION;

    /**
     * Base to multiply.
     */
    @Getter
    @Setter
    private int base = DEFAULT_BASE;

    @Getter
    @Setter
    private int multiplier = DEFAULT_MULT;

    /**
     * Portion of the number to be randomized, between 0 and 1.
     * 0 - no random portion, 1 - randomize everything.
     */
    @Getter
    @Setter
    private float randomPortion = DEFAULT_RANDOM_PORTION;

    /**
     * Additional fixed retry time in milliseconds for each retry.
     */
    @Getter
    @Setter
    private long extraWait = 0;

    /**
     * Maximum duration at which the wait time needs to be capped.
     */
    @Getter
    @Setter
    private Duration maxRetryThreshold = Duration.ZERO;

    public ExponentialBackoffRetry(IRetryable runFunction) {
        super(runFunction);
    }

    @Override
    public boolean stopRequested() {
        return stop;
    }

    @Override
    public void nextWait() {
        // maxRetryThreshold is cap at how long to sleep, if its 5 seconds and sleepTime exceeds it, it will be capped at 5 seconds.

        if (nextBackoffTime == 0) {
            nextBackoffTime = System.currentTimeMillis() + backoffDuration.toMillis();
        }
        retryCounter++;
        long sleepTime = base * (long) Math.pow(multiplier, retryCounter);

        sleepTime += extraWait;
        float randomPart = new Random().nextFloat() * randomPortion;
        sleepTime -= sleepTime * randomPart;
        if (maxRetryThreshold.toMillis() > 0) {
            sleepTime = Math.min(sleepTime, maxRetryThreshold.toMillis());
        }

        if (System.currentTimeMillis() + sleepTime > nextBackoffTime) {
            nextBackoffTime = 0;
            retryCounter = 1;
            sleepTime = base + extraWait;
            sleepTime -= sleepTime * randomPart;
            if (stopAfterBackoffDurationExceeded) {
                stop = true;
            }
        }
        Sleep.sleepUninterruptibly(Duration.ofMillis(sleepTime));
    }

}
