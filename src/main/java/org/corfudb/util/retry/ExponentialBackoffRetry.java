package org.corfudb.util.retry;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Created by mwei on 9/1/15.
 */
@Slf4j
@RequiredArgsConstructor(staticName="build")
public class ExponentialBackoffRetry implements IRetry {

    @Getter
    long retryCounter = 0;

    @Getter
    LocalDateTime backoffTime = null;

    @Getter
    final IRetryable runFunction;

    boolean exponentialRetry() {
        if (backoffTime == null)
        {
            backoffTime = LocalDateTime.now();
            retryCounter++;
        }
        else if (backoffTime.isAfter(LocalDateTime.now().minus((long)Math.pow(10, retryCounter), ChronoUnit.MILLIS)))
        {
            log.info("Backoff reset to 0.");
            backoffTime = LocalDateTime.now();
            retryCounter = 0;
        }
        else
        {
            retryCounter++;
            try {
                Thread.sleep((long) Math.pow(10, retryCounter));
            } catch (InterruptedException ie)
            {

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
    public boolean handleException(Exception e) {
        log.warn("Exception occurred during running of retry, backoff=" + retryCounter, e);
        return exponentialRetry();
    }

}
