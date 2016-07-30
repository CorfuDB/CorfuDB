package org.corfudb.util.retry;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements a basic exponential backoff retry.
 * <p>
 * Created by mwei on 9/1/15.
 */
@Slf4j
public class ExponentialBackoffRetry<E extends Exception, F extends Exception, G extends Exception, H extends Exception, O, A extends IRetry> implements IRetry<E, F, G, H, O, ExponentialBackoffRetry> {

    @Getter
    final IRetryable<E, F, G, H, O> runFunction;
    @Getter
    final Map<Class<? extends Exception>, ExceptionHandler> handlerMap = new HashMap<>();
    @Getter
    long retryCounter = 0;
    @Getter
    LocalDateTime backoffTime = null;

    public ExponentialBackoffRetry(IRetryable runFunction) {
        this.runFunction = runFunction;
    }

    boolean exponentialRetry() {
        if (backoffTime == null) {
            backoffTime = LocalDateTime.now();
            retryCounter++;
        } else if (backoffTime.isAfter(LocalDateTime.now().minus((long) Math.pow(10, retryCounter), ChronoUnit.MILLIS))) {
            backoffTime = LocalDateTime.now();
            retryCounter = 0;
        } else {
            retryCounter++;
            try {
                Thread.sleep((long) Math.pow(10, retryCounter));
            } catch (InterruptedException ie) {

            }
        }
        return true;
    }

    /**
     * Handle an exception which has occurred and that has not been registered.
     *
     * @param e The exception that has occurred.
     * @return True, to continue retrying, or False, to stop running the function.
     */
    @Override
    public boolean handleException(Exception e, boolean unhandled) {
        if (unhandled) {
            log.warn("Exception occurred during running of retry, backoff=" + retryCounter, e);
        }
        return exponentialRetry();
    }

    /**
     * Apply the retry logic.
     *
     * @return True, if we should continue retrying, false otherwise.
     */
    @Override
    public boolean retryLogic() {
        return exponentialRetry();
    }

}
