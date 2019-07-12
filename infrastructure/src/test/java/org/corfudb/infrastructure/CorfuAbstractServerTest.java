package org.corfudb.infrastructure;

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
        final CorfuMsgHandler handler = mock(CorfuMsgHandler.class);
        final ExecutorService executor = mock(ExecutorService.class);

        AbstractServer server = new AbstractServer() {
            @Override
            public CorfuMsgHandler getHandler() {
                return handler;
            }

            @Override
            public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
                return getState() == ServerState.READY;
            }

            @Override
            public ExecutorService getExecutor(CorfuMsgType corfuMsgType) {
                return executor;
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
    }
}
