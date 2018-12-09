/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.util.retry;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

/**
 * Created by Konstantin Spirov on 4/6/2017.
 */
public class IRetryTest extends AbstractCorfuTest {

    public static final long MAX_SLEEP_TIME = 64L;
    public static final int BASE = 2;

    @Test
    public void testIRetryReturnsValueAfterLotsOfRetries() throws SQLException, InterruptedException {
        AtomicInteger retries = new AtomicInteger(0);
        String e = IRetry.build(ExponentialBackoffRetry.class, SQLException.class, () -> {
            if (retries.getAndIncrement()< PARAMETERS.NUM_ITERATIONS_MODERATE) {
                throw new RetryNeededException();
            }
            return "ok";
        }).setOptions(x -> x.setBase(1))
        .run();
        assertThat(e).isEqualTo("ok");
        assertThat(retries.get()).isEqualTo(PARAMETERS.NUM_ITERATIONS_MODERATE+1);
    }

    @Test(expected = SQLException.class)
    public void testIRetryIsAbleToThrowCatchedExceptions()
            throws SQLException, InterruptedException {
        IRetry.build(ExponentialBackoffRetry.class, SQLException.class, () -> {
            throw new SQLException();
        }).run();
    }

    @Test
    public void testExponentialBackoffPattern() {
        // Max duration of a sleep in milliseconds (power of 2 to test exponential calculator).
        long maxSleepTimeMs = MAX_SLEEP_TIME;

        // Number of expected exponential backoff's before it becomes constant
        int expectedOccurrences = (int) (Math.log(maxSleepTimeMs) / Math.log(BASE));

        ExponentialBackoffRetry backoffCalculator = new ExponentialBackoffRetry(null,
                Duration.ofMillis(maxSleepTimeMs));

        Duration currentSleepTime = Duration.ZERO;

        // Assert time is exponentially growing
        for (int i=0; i < expectedOccurrences; i++) {
            currentSleepTime = backoffCalculator.nextWait();
            assertThat((double)currentSleepTime.toMillis()).isEqualTo(Math.pow(BASE, i+1));
        }

        // Assert it has reached upper limit
        assertThat(currentSleepTime.toMillis()).isEqualTo(maxSleepTimeMs);

        // Assert maxRetry stays constant
        for (int i=0; i < expectedOccurrences; i++) {
            assertThat(currentSleepTime).isEqualTo(backoffCalculator.nextWait());
        }
    }

}