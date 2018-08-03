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

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR;
    }

    private ServerContext getContext() {
        String path = getDirPath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

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
        StreamLogCompaction compaction = new StreamLogCompaction(streamLog, 10, 10, TimeUnit.MILLISECONDS);
        Thread.sleep(35);
        compaction.shutdown();

        assertEquals(3, compaction.getGcCounter());
    }
}