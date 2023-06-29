package org.corfudb.util.retry;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This retry class is a mix between interval retry and exponential backoff retry with the finite
 * retry duration. When the RetryNeededException is throw inside of the retry loop the interval retry
 * strategy is used to wait for lastDuration. If the BackoffRetryNeededException is thrown, the exponential
 * backoff strategy is used, which subsequently updates the lastDuration. The parameter overallMaxRetryDuration
 * is set to limit the overall retry duration. After the overallMaxRetryDuration, the RetryExhaustedException
 * will be thrown inside of the run() method.
 */
public class MixedBoundRetry<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, O> extends AbstractRetry<E, F,
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

    /**
     * Completes and throws RetryExhaustedException on get after overallMaxRetryDuration is reached
     * after run has started.
     */
    @Getter
    Optional<ScheduledFuture<?>> endSessionFuture = Optional.empty();

    public MixedBoundRetry(IRetryable<E, F, G, H, O> runFunction) {
        super(runFunction);
    }

    private void waitUntilSessionExpires() {
        endSessionFuture.ifPresent(f -> CFUtils.getUninterruptibly(f, RetryExhaustedException.class));
    }

    private long getRemainingSessionTime() {
        return endSessionFuture.map(f -> f.getDelay(TimeUnit.MILLISECONDS))
                .orElseThrow(() -> new IllegalStateException("Scheduler is not started"));
    }

    private void expireSessionIfSleepExceedsDelay(long sleepTime) {
        if (sleepTime > getRemainingSessionTime()) {
            waitUntilSessionExpires();
        }
    }

    private void cleanupIfSessionExpired(ScheduledExecutorService scheduler) {
        endSessionFuture.map(Future::isDone).ifPresent(expired -> {
            if (expired) {
                scheduler.shutdown();
            }
        });
    }

    @Override
    public void nextWait() {
        if (maxRetryDuration.toMillis() > 0) {
            lastDuration = Duration.ofMillis(Math.min(lastDuration.toMillis(), maxRetryDuration.toMillis()));
        }
        expireSessionIfSleepExceedsDelay(lastDuration.toMillis());
        Sleep.sleepUninterruptibly(lastDuration);
    }

    private void backOffWait() {
        backOffCounter += 1;
        long sleepTime = baseDuration.toMillis() * (long) Math.pow(multiplier, backOffCounter);
        sleepTime -= sleepTime * new Random().nextFloat() * randomPart;
        if (maxRetryDuration.toMillis() > 0) {
            sleepTime = Math.min(sleepTime, maxRetryDuration.toMillis());
        }
        expireSessionIfSleepExceedsDelay(sleepTime);
        lastDuration = Duration.ofMillis(sleepTime);
        Sleep.sleepUninterruptibly(lastDuration);
    }

    @Override
    public O run() throws E, F, G, H, InterruptedException {
        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(1);
        endSessionFuture = Optional.of(scheduler.schedule(() -> {
            throw new RetryExhaustedException();
        }, overallMaxRetryDuration.toMillis(), TimeUnit.MILLISECONDS));
        lastDuration = baseDuration;
        while (true) {
            try {
                return getRunFunction().retryFunction();
            } catch (RetryNeededException ex) {
                nextWait();
            } catch (BackoffRetryNeededException ex) {
                backOffWait();
            } finally {
                cleanupIfSessionExpired(scheduler);
            }
        }
    }


    public static class BackoffRetryNeededException extends RuntimeException {

    }

}
