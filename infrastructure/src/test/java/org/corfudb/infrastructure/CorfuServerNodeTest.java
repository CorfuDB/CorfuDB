package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by cgudisagar on 1/25/24.
 */
public class CorfuServerNodeTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // Objects that need to be mocked
    private ServerContext mockServerContext;

    /**
     * Perform the required preparation before running individual tests.
     * This includes preparing the mocks and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {
        mockServerContext = mock(ServerContext.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread
        when(mockServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());
    }
    @Test
    public void testRestartServerChannel() throws InterruptedException {
        CorfuServerNode node = new CorfuServerNode(mockServerContext, ImmutableMap.of());

        // spy CountDownLatch to verify countdown() call made during restartServerChannel()
        CountDownLatch countDownLatch = spy(new CountDownLatch(1));

        CorfuServer.setResetLatch(countDownLatch);
        CorfuServer.getReloadServerComponents().set(true);
        node.restartServerChannel();

        // Wait for the latch to be signalled from restartServerChannel()
        countDownLatch.await();

        // Verify that refresh worker group was indeed called once.
        // This is important to verify that the server threads were refreshed
        // during channel restart.
        verify(mockServerContext,times(1)).refreshWorkerGroupThreads();

        // Verify spy CountDownLatch invocations
        verify(countDownLatch,times(1)).countDown();
        verify(countDownLatch,times(1)).await();
        Assertions.assertThat(CorfuServer.getReloadServerComponents().get()).isFalse();
    }
}
