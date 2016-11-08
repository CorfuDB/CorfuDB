package org.corfudb;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
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
        scheduleConcurrently(5, t -> l.getAndIncrement());
        executeScheduled(5, 1000, TimeUnit.MILLISECONDS);
        assertThat(l.get())
                .isEqualTo(5);
    }

    @Test
    public void concurrentTestsThrowExceptions()
            throws Exception {
        scheduleConcurrently(5, t -> {
            throw new IOException("hi");
        });
        assertThatThrownBy(() -> executeScheduled(5, 1000, TimeUnit.MILLISECONDS))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void concurrentTestsTimeout()
            throws Exception {
        scheduleConcurrently(5, t -> Thread.sleep(10000));
        assertThatThrownBy(() -> executeScheduled(5, 1, TimeUnit.MILLISECONDS))
                .isInstanceOf(CancellationException.class);
    }

}
