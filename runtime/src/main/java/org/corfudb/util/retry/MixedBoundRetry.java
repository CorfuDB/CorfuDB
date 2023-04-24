package org.corfudb.util.retry;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Random;

/**
 * This retry class is a mix between interval retry and exponential backoff retry with the finite
 * retry duration. When the RetryNeededException is throw inside of the retry loop the interval retry
 * strategy is used to wait for lastDuration. If the BackoffRetryNeededException is thrown, the exponential
 * backoff strategy is used, which subsequently updates the lastDuration. The parameter overallMaxRetryDuration
 * is set to limit the overall retry duration. After the overallMaxRetryDuration, the RetryExhaustedException
 * will be thrown inside of the run() method.
 */
public class MixedBoundRetry<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, O, A extends IRetry> extends AbstractRetry<E, F,
        G, H, O, MixedBoundRetry> {

    /**
     * Overall duration of the retry
     */
    @Setter
    @Getter
    @NonNull
    Duration overallMaxRetryDuration = Duration.ofSeconds(5);

    /**
     * The max duration of a single retry, after which the retry duration will stop growing.
     */
    @Setter
    @Getter
    @NonNull
    Duration maxRetryDuration = Duration.ofSeconds(2);

    /**
     * The initial duration for the interval retry and the base duration for the exponential backoff retry.
     */
    @Setter
    @Getter
    @NonNull
    Duration baseDuration = Duration.ofSeconds(1);

    /**
     * The base of the exponential backoff growth function
     */
    @Setter
    @Getter
    int multiplier = 2;

    /**
     * Randomization factor from 0-1 to adjust the exponential backoff duration.
     */
    @Setter
    @Getter
    float randomPart = 0f;

    /**
     * Last waited duration
     */
    @Getter
    @NonNull
    Duration lastDuration = baseDuration;

    /**
     * Number of backoff retries requested
     */
    @Getter
    int backOffCounter = 0;

    /**
     * When will the retry loop end
     */
    @Getter
    long endSession = 0;


    public MixedBoundRetry(IRetryable<E, F, G, H, O> runFunction) {
        super(runFunction);
    }


    @Override
    public void nextWait() {
        if (System.currentTimeMillis() + lastDuration.toMillis() > endSession) {
            throw new RetryExhaustedException();
        }
        if (maxRetryDuration.toMillis() > 0) {
            lastDuration = Duration.ofMillis(Math.min(lastDuration.toMillis(), maxRetryDuration.toMillis()));
        }
        Sleep.sleepUninterruptibly(lastDuration);
    }

    private void backOffWait() {
        backOffCounter += 1;
        long sleepTime = baseDuration.toMillis() * (long) Math.pow(multiplier, backOffCounter);
        sleepTime -= sleepTime * new Random().nextFloat() * randomPart;
        if (maxRetryDuration.toMillis() > 0) {
            sleepTime = Math.min(sleepTime, maxRetryDuration.toMillis());
        }
        if (System.currentTimeMillis() + sleepTime > endSession) {
            throw new RetryExhaustedException();
        }
        lastDuration = Duration.ofMillis(sleepTime);
        Sleep.sleepUninterruptibly(lastDuration);
    }

    @Override
    public O run() throws E, F, G, H {
        endSession = System.currentTimeMillis() + overallMaxRetryDuration.toMillis();
        lastDuration = baseDuration;
        while (true) {
            try {
                return getRunFunction().retryFunction();
            } catch (RetryNeededException ex) {
                nextWait();
            } catch (BackoffRetryNeededException ex) {
                backOffWait();
            }
        }
    }


    public static class BackoffRetryNeededException extends RuntimeException {

    }

}
