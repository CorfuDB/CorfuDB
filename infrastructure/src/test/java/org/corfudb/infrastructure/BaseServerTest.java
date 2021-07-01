package org.corfudb.infrastructure;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getPingRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getResetRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getRestartRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class BaseServerTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The BaseServer instance used for testing
    private BaseServer baseServer;

    // Objects that need to be mocked
    private ServerContext mockServerContext;
    private IServerRouter mockServerRouter;
    private ChannelHandlerContext mockChannelHandlerContext;

    private final AtomicInteger requestCounter = new AtomicInteger();

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     *
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that compares the base fields of two message headers.
     * These include the request ID, the epoch, the client ID, and the cluster ID.
     * @param requestHeader   the header from the request message
     * @param responseHeader  the header from the response message
     * @return                true if the two headers have the same base field values
     */
    private boolean compareBaseHeaderFields(HeaderMsg requestHeader, HeaderMsg responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    /**
     * Perform the required preparation before running individual tests.
     * This includes preparing the mocks and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {
        mockServerContext = mock(ServerContext.class);
        mockServerRouter = mock(IServerRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread
        when(mockServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());
        when(mockServerContext.getConfiguration()).thenReturn(new ServerConfiguration());

        baseServer = new BaseServer(mockServerContext);
    }

    /**
     * Test that the BaseServer correctly handles a PING_REQUEST.
     */
    @Test
    public void testPing() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getPingRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        baseServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter).sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a PING_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasPingResponse());
    }

    /**
     * Test that the BaseServer correctly handles a SEAL_REQUEST. If the
     * SEAL epoch is greater than or equal to the current server epoch,
     * a typical SEAL_RESPONSE should be received.
     */
    @Test
    public void testSealValidEpoch() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSealRequestMsg(1L)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        doNothing().when(mockServerContext).setServerEpoch(anyLong(), any(IServerRouter.class));
        baseServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter).sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a SEAL_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasSealResponse());
    }

    /**
     * Test that the BaseServer correctly handles a SEAL_REQUEST. If the
     * SEAL epoch is smaller than the current server epoch, a WRONG_EPOCH
     * error should be received.
     */
    @Test
    public void testSealWrongEpoch() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSealRequestMsg(1L)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        doThrow(new WrongEpochException(2L)).when(mockServerContext)
                .setServerEpoch(anyLong(), any(IServerRouter.class));

        baseServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter).sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that we receive a WRONG_EPOCH error and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasWrongEpochError());
        assertEquals(2L, response.getPayload().getServerError().getWrongEpochError().getCorrectEpoch());
    }

    /**
     * Test that the BaseServer correctly handles a RESET_REQUEST.
     */
    @Test
    public void testReset() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getResetRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        try (MockedStatic<CorfuServer> corfuServerMockedStatic = Mockito.mockStatic(CorfuServer.class)) {
            baseServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

            verify(mockServerRouter).sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
            ResponseMsg response = responseCaptor.getValue();

            // Assert that the payload has a RESET_RESPONSE and that the base
            // header fields have remained the same
            assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
            assertTrue(response.getPayload().hasResetResponse());

            // Assert that restartServer has been called exactly once with resetData = true
            corfuServerMockedStatic.verify(() -> CorfuServer.restartServer(true));
        }
    }

    /**
     * Test that the BaseServer correctly handles a RESTART_REQUEST.
     */
    @Test
    public void testRestart() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getRestartRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        try (MockedStatic<CorfuServer> corfuServerMockedStatic = Mockito.mockStatic(CorfuServer.class)) {
            baseServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

            verify(mockServerRouter).sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
            ResponseMsg response = responseCaptor.getValue();

            // Assert that the payload has a RESET_RESPONSE and that the base
            // header fields have remained the same
            assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
            assertTrue(response.getPayload().hasRestartResponse());

            // Assert that restartServer has been called exactly once with resetData = false
            corfuServerMockedStatic.verify(() -> CorfuServer.restartServer(false));
        }
    }
}
