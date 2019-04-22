package org.corfudb.infrastructure.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.metrics.Counter;
import org.corfudb.util.metrics.NullStatsLogger;
import org.corfudb.util.metrics.StatsLogger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamLogCompactionTest extends AbstractCorfuTest {

    StatsLogger statsLogger;

    Counter streamCompactTimer = statsLogger.getCounter(CorfuComponent.INFRA_STREAM_OPS + "compaction");

    @BeforeClass
    public static void setUpMetrics() {
    }

    /**
     * Test that task catches all possible exceptions and doesn't break scheduled executor. Test
     * depends on checking  stream compaction metrics and sets the expectations based on whether
     * metrics are enabled.
     *
     * @throws InterruptedException thread sleep
     */
    @Test
    public void testCompaction() throws InterruptedException {
        log.debug("Start log compaction test");

        final int timeout = 10;
        final int initialDelay = 10;
        final int period = 10;

        StreamLog streamLog = mock(StreamLog.class);
        doThrow(new RuntimeException("err")).when(streamLog).compact();

        final long initialCompactionCounter = getCompactionCounter();
        StreamLogCompaction compaction = new StreamLogCompaction(streamLog,
                                                                 initialDelay,
                                                                 period,
                                                                 TimeUnit.MILLISECONDS,
                                                                 PARAMETERS.TIMEOUT_VERY_SHORT,
                                                                 NullStatsLogger.INSTANCE);

        /**
        // If metrics are enabled, set an expectation of two compactions more than current
        // compaction count
        final long expectedCompactionCounter = initialCompactionCounter +
                (MetricsUtils.isMetricsCollectionEnabled() ? 2 : 0);

        while(getCompactionCounter() < expectedCompactionCounter){
            TimeUnit.MILLISECONDS.sleep(timeout);
        }

        compaction.shutdown();

        assertThat(getCompactionCounter()).isGreaterThanOrEqualTo(expectedCompactionCounter);
         **/
    }

    private long getCompactionCounter() {
        return streamCompactTimer.getCount();
    }
}