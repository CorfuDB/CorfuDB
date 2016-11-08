package org.corfudb.util.retry;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class implements a basic interval-based retry, with sentinel.
 * <p>
 * Created by mwei on 9/1/15.
 */
@Slf4j
public class IntervalAndSentinelRetry<E extends Exception, F extends Exception, G extends Exception, H extends Exception, O> implements IRetry<E, F, G, H, O, IntervalAndSentinelRetry> {

    @Getter
    final IRetryable<E, F, G, H, O> runFunction;
    @Getter
    final Map<Class<? extends Exception>, ExceptionHandler> handlerMap = new HashMap<>();
    /**
     * The interval, in milliseconds to wait for retry
     **/
    @Getter
    @Setter
    long retryInterval = 0;
    /**
     * The sentinel boolean reference. The sentinel should be set to true if we should
     * continue retrying, false otherwise.
     */
    @Getter
    @Setter
    AtomicBoolean sentinelReference;

    public IntervalAndSentinelRetry(IRetryable runFunction) {
        this.runFunction = runFunction;
    }

    /**
     * Handle an exception which has occurred and that has not been registered.
     *
     * @param e The exception that has occurred.
     * @return True, to continue retrying, or False, to stop running the function.
     */
    @Override
    public boolean handleException(Exception e, boolean unhandled) {
        return retryLogic();
    }

    /**
     * Return the value of the sentinel, if set.
     * If the sentinel is not set, true is returned.
     *
     * @return The value of the sentinel, or true, if no sentinel was set.
     */
    boolean checkSentinel() {
        return sentinelReference == null || sentinelReference.get();
    }

    /**
     * Apply the retry logic.
     *
     * @return True, if we should continue retrying, false otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    @SneakyThrows
    public boolean retryLogic() {
        if (!checkSentinel()) {
            return false;
        }
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException ie) {
            //actually pass this up to the handler, in case interruptedexception was listened on.
            if (handlerMap.containsKey(InterruptedException.class)) {
                return handlerMap.get(InterruptedException.class).HandleException(ie, this);
            }
        }
        return checkSentinel();
    }

}
