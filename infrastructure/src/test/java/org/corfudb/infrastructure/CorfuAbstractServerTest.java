package org.corfudb.infrastructure;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class CorfuAbstractServerTest {

    /**
     * Check that handler is not executed if the server  is in shutdown state.
     */
    @Test
    public void testShutdown() {
        final HandlerMethods handler = mock(HandlerMethods.class);
        final ExecutorService executor = mock(ExecutorService.class);

        AbstractServer server = new AbstractServer() {
            @Override
            public HandlerMethods getHandlerMethods() {
                return handler;
            }

            @Override
            public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
                return getState() == ServerState.READY;
            }

            @Override
            public List<ExecutorService> getExecutors() {
                return Collections.singletonList(executor);
            }
        };

        server.shutdown();

        server.handleMessage(
                new CorfuMsg(CorfuMsgType.WRITE),
                mock(ChannelHandlerContext.class),
                mock(IServerRouter.class)
        );

        verify(handler, times(0)).execute(any(), any(), any());
    }
}
