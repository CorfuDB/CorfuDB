package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@Slf4j
public class StreamLogCompactionTest extends AbstractCorfuTest {
    private final long initialDelay = 10;
    private final long period = 10;
    private final long timeout = 35;
    private final int expectedCompactCounter = 3;

    /**
     * Test that task catch all possible exceptions and doesn't break scheduled executor
     *
     * @throws InterruptedException thread sleep
     */
    @Test
    public void testCompaction() throws InterruptedException {
        log.debug("Start log compaction test");

        StreamLog streamLog = mock(StreamLog.class);
        doThrow(new RuntimeException("err")).when(streamLog).compact();
        StreamLogCompaction compaction = new StreamLogCompaction(streamLog, initialDelay, period, TimeUnit.MILLISECONDS);
        Thread.sleep(timeout);
        compaction.shutdown();

        assertEquals(expectedCompactCounter, compaction.getGcCounter());
    }
}