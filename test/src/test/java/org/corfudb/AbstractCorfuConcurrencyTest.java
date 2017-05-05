package org.corfudb;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 2/9/16.
 */
public class AbstractCorfuConcurrencyTest extends AbstractCorfuTest {


    @Test
    public void concurrentTestsExecute()
            throws Exception {
        final AtomicLong l = new AtomicLong();
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME, t ->
                l.getAndIncrement());
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_SHORT);
        assertThat(l.get())
                .isEqualTo(PARAMETERS.CONCURRENCY_SOME);
    }

    @Test
    public void concurrentTestsThrowExceptions()
            throws Exception {
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME, t -> {
            throw new IOException("hi");
        });
        assertThatThrownBy(() -> executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_SHORT))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void concurrentTestsTimeout()
            throws Exception {
        scheduleConcurrently(PARAMETERS.CONCURRENCY_SOME,
                t -> Thread.sleep(PARAMETERS.TIMEOUT_LONG.toMillis()));
        assertThatThrownBy(() -> executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_VERY_SHORT))
                .isInstanceOf(CancellationException.class);
    }

}
