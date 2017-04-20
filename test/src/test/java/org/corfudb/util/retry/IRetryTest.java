/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.util.retry;

import org.corfudb.AbstractCorfuTest;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by Konstantin Spirov on 4/6/2017.
 */
public class IRetryTest extends AbstractCorfuTest {

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

    @Test
    public void testIRetryIsAbleToThrowCatchedExceptions() throws InterruptedException {
        AtomicInteger retries = new AtomicInteger(0);
        try {
            String e = IRetry.build(ExponentialBackoffRetry.class, SQLException.class, () -> {
                if (true) throw new SQLException();
                return "";
            }).run();
            fail("Exception not propagated");
        } catch (SQLException e) {
            // expected
        }
    }

}