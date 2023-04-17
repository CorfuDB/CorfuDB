package org.corfudb.util.retry;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Random;

public class MixedBoundRetry<E extends Exception, F extends Exception,
        G extends Exception, H extends Exception, O, A extends IRetry> extends AbstractRetry<E, F,
        G, H, O, MixedBoundRetry> {

    @Setter
    @Getter
    @NonNull
    Duration overallMaxRetryDuration = Duration.ofSeconds(5);

    @Setter
    @Getter
    @NonNull
    Duration maxRetryDuration = Duration.ofSeconds(2);

    @Setter
    @Getter
    @NonNull
    Duration baseDuration = Duration.ofSeconds(1);

    @Setter
    @Getter
    int multiplier = 2;

    @Setter
    @Getter
    float randomPart = 0f;

    @Getter
    @NonNull
    Duration lastDuration = baseDuration;

    @Getter
    int backOffCounter = 0;

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
