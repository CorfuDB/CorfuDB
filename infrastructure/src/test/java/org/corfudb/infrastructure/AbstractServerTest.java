package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.ExecutorService;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.junit.Test;

import static org.corfudb.protocols.service.CorfuProtocolBase.getPingRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractServerTest {

    /**
     * Check that handler is not executed if the server is in shutdown state.
     */
    @Test
    public void testShutdown() {
        final RequestHandlerMethods handlerMethods = mock(RequestHandlerMethods.class);
        final ExecutorService executor = mock(ExecutorService.class);

        AbstractServer server = new AbstractServer() {
            @Override
            public RequestHandlerMethods getHandlerMethods() {
                return handlerMethods;
            }

            @Override
            protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
                executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
            }
        };

        server.shutdown();

        server.handleMessage(
                getRequestMsg(HeaderMsg.getDefaultInstance(), getPingRequestMsg()),
                mock(ChannelHandlerContext.class),
                mock(IServerRouter.class)
        );

        verify(executor, times(0)).submit(any(Runnable.class));
        verify(handlerMethods, times(0)).handle(any(), any(), any());
    }
}
