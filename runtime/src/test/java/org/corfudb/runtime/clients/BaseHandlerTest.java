package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getBootstrappedErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getNotBootstrappedErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getNotReadyErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongClusterErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongEpochErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getUnknownErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getPingResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getResetResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getRestartResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getSealResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class BaseHandlerTest {

    // The BaseHandler instance used for testing
    private BaseHandler baseHandler;

    // Objects that need to be mocked
    private IClientRouter mockClientRouter;
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
     * Perform the required preparation before running individual
     * tests by preparing the mocks.
     */
    @Before
    public void setup() {
        mockClientRouter = mock(IClientRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        baseHandler = new BaseHandler();
        baseHandler.setRouter(mockClientRouter);
    }

    /**
     * Test that the BaseHandler correctly handles a PING_RESPONSE.
     */
    @Test
    public void testHandlePing() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPingResponseMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the BaseHandler correctly handles a RESTART_RESPONSE.
     */
    @Test
    public void testHandleRestart() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getRestartResponseMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the BaseHandler correctly handles a RESET_RESPONSE.
     */
    @Test
    public void testHandleReset() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getResetResponseMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the BaseHandler correctly handles a SEAL_RESPONSE.
     */
    @Test
    public void testHandleSeal() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSealResponseMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the BaseHandler correctly handles a WRONG_EPOCH error.
     */
    @Test
    public void testHandleWrongEpochError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getWrongEpochErrorMsg(2L)
        );

        ArgumentCaptor<WrongEpochException> exceptionCaptor = ArgumentCaptor.forClass(WrongEpochException.class);
        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), exceptionCaptor.capture());

        assertEquals(2L, exceptionCaptor.getValue().getCorrectEpoch());
    }

    /**
     * Test that the BaseHandler correctly handles a WRONG_CLUSTER error.
     */
    @Test
    public void testHandleWrongClusterError() {
        final UUID EXPECTED_UUID = UUID.randomUUID();
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getWrongClusterErrorMsg(getUuidMsg(EXPECTED_UUID), getUuidMsg(DEFAULT_UUID))
        );

        ArgumentCaptor<WrongClusterException> exceptionCaptor = ArgumentCaptor.forClass(WrongClusterException.class);
        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), exceptionCaptor.capture());

        assertEquals(EXPECTED_UUID, exceptionCaptor.getValue().getExpectedCluster());
        assertEquals(DEFAULT_UUID, exceptionCaptor.getValue().getActualCluster());
    }

    /**
     * Test that the BaseHandler correctly handles a NOT_READY error.
     */
    @Test
    public void testHandleNotReadyError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getNotReadyErrorMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), any(ServerNotReadyException.class));
    }

    /**
     * Test that the BaseHandler correctly handles a BOOTSTRAPPED error.
     */
    @Test
    public void testHandleBootstrappedError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getBootstrappedErrorMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), any(AlreadyBootstrappedException.class));
    }

    /**
     * Test that the BaseHandler correctly handles a NOT_BOOTSTRAPPED error.
     */
    @Test
    public void testHandleNotBootstrappedError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getNotBootstrappedErrorMsg()
        );

        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), any(NoBootstrapException.class));
    }

    /**
     * Test that the BaseHandler correctly handles a UNKNOWN_ERROR error.
     */
    @Test
    public void testHandleUnknownError() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getUnknownErrorMsg(new Exception("Unknown Exception Test"))
        );

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        baseHandler.handleMessage(response, mockChannelHandlerContext);

        // Verify that the correct request was completed exceptionally (once)
        // with the expected exception
        verify(mockClientRouter, never()).completeRequest(anyLong(), any());
        verify(mockClientRouter).completeExceptionally(
                eq(response.getHeader().getRequestId()), exceptionCaptor.capture());

        assertEquals("Unknown Exception Test", exceptionCaptor.getValue().getMessage());
    }
}
