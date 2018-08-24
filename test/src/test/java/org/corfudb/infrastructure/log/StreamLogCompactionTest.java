package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@Slf4j
public class StreamLogCompactionTest extends AbstractCorfuTest {

    /**
     * Test that task catch all possible exceptions and doesn't break scheduled executor
     *
     * @throws InterruptedException thread sleep
     */
    @Test
    public void testCompaction() throws InterruptedException {
        log.debug("Start log compaction test");

        final int timeout = 35;
        final int initialDelay = 10;
        final int period = 10;

        StreamLog streamLog = mock(StreamLog.class);
        doThrow(new RuntimeException("err")).when(streamLog).compact();

        StreamLogCompaction compaction = new StreamLogCompaction(
                streamLog, initialDelay, period, TimeUnit.MILLISECONDS, Duration.ofSeconds(1)
        );

        TimeUnit.MILLISECONDS.sleep(timeout);
        compaction.shutdown();

        long gcCounter = ServerContext.getMetrics()
                .getCounters()
                .get(StreamLogCompaction.STREAM_COMPACT_METRIC)
                .getCount();

        final long expectedGcCounter = 2;
        assertThat(gcCounter).isGreaterThanOrEqualTo(expectedGcCounter);
    }
}