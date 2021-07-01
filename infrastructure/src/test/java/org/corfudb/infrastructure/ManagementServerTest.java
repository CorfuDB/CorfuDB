package org.corfudb.infrastructure;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.infrastructure.orchestrator.Orchestrator;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.proto.NodeConnectivity.NodeConnectivityType;
import org.corfudb.runtime.proto.RpcCommon.LayoutMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg.SequencerStatus;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Management.BootstrapManagementRequestMsg;
import org.corfudb.runtime.proto.service.Management.QueryNodeResponseMsg;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealFailureRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getManagementLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryWorkflowRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class ManagementServerTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The ManagementServer instance used for testing.
    private ManagementServer managementServer;

    // Objects that need to be mocked.
    private ServerContext mServerContext;
    private IServerRouter mServerRouter;
    private ChannelHandlerContext mChannelHandlerContext;
    private ClusterState mClusterState;
    private Orchestrator mOrchestrator;
    private IReconfigurationHandlerPolicy mFailureHandlerPolicy;
    private CorfuRuntime mCorfuRuntime;

    private final AtomicInteger requestCounter = new AtomicInteger();
    private final List<String> NODES = Arrays.asList("localhost:9000", "localhost:9001", "localhost:9002");

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     * @param ignoreClusterId   indicates if the message is clusterId aware
     * @param ignoreEpoch       indicates if the message is epoch aware
     * @return                  the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 1L,
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
     * A helper method that creates a Layout with a single segment having
     * a single stripe.
     * @param layoutServers         a list of layout servers
     * @param sequencers            a list of sequencers
     * @param logServers            a list of log unit servers
     * @param unresponsiveServers   a list of unresponsive servers
     * @param epoch                 the Layout epoch
     * @param clusterId             the clusterId
     * @return                      a newly created Layout
     */
    private Layout getBasicLayout(@NonNull List<String> layoutServers, @NonNull List<String> sequencers,
                                  @Nonnull List<String> logServers, @NonNull List<String> unresponsiveServers,
                                  long epoch, @Nullable UUID clusterId) {

        Layout.LayoutSegment segment = new Layout.LayoutSegment(
                ReplicationMode.CHAIN_REPLICATION,0, -1,
                Collections.singletonList(new Layout.LayoutStripe(logServers))
        );

        return new Layout(layoutServers, sequencers,
                Collections.singletonList(segment), unresponsiveServers, epoch, clusterId);
    }

    /**
     * A helper method that creates a basic 3 node Layout with a single
     * segment having a single stripe.
     * @param epoch       the Layout epoch
     * @param clusterId   the clusterId
     * @return            a newly created Layout
     */
    private Layout getBasicLayout(long epoch, @Nullable UUID clusterId) {
        return getBasicLayout(NODES, NODES, NODES, Collections.emptyList(), epoch, clusterId);
    }

    /**
     * Perform the required preparation before running individual tests.
     * This includes preparing the mocks and initializing the DirectExecutorService.
     */
    @Before
    public void setup() {
        mServerContext = mock(ServerContext.class);
        mServerRouter = mock(IServerRouter.class);
        mChannelHandlerContext = mock(ChannelHandlerContext.class);
        mClusterState = mock(ClusterState.class);
        mOrchestrator = mock(Orchestrator.class);
        mFailureHandlerPolicy = mock(IReconfigurationHandlerPolicy.class);
        mCorfuRuntime = mock(CorfuRuntime.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread.
        when(mServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        when(mServerContext.getLocalEndpoint()).thenReturn(NODES.get(0));
        when(mServerContext.getFailureHandlerPolicy()).thenReturn(mFailureHandlerPolicy);
        when(mServerContext.getConfiguration()).thenReturn(new ServerConfiguration());

        ClusterStateContext mClusterContext = mock(ClusterStateContext.class);
        when(mClusterContext.getClusterView()).thenReturn(mClusterState);
        ManagementAgent mManagementAgent = mock(ManagementAgent.class);
        when(mManagementAgent.getCorfuRuntime()).thenReturn(mCorfuRuntime);
        SingletonResource<CorfuRuntime> mSingleton = mock(SingletonResource.class);

        // Prepare the ManagementServerInitializer.
        ManagementServer.ManagementServerInitializer mMSI = mock(ManagementServer.ManagementServerInitializer.class);
        when(mMSI.buildDefaultClusterContext(mServerContext)).thenReturn(mClusterContext);
        when(mMSI.buildManagementAgent(mSingleton, mServerContext, mClusterContext)).thenReturn(mManagementAgent);
        when(mMSI.buildOrchestrator(mSingleton, mServerContext)).thenReturn(mOrchestrator);

        try (MockedStatic<SingletonResource> singletonMocked = mockStatic(SingletonResource.class)) {
            singletonMocked.when(() -> SingletonResource.withInitial(ArgumentMatchers.<Supplier<CorfuRuntime>>any()))
                    .thenReturn(mSingleton);

            managementServer = new ManagementServer(mServerContext, mMSI);
        }
    }

    /**
     *  Test that the ManagementServer correctly handles an ORCHESTRATOR request.
     *  The ManagementServer should forward this request to the Orchestrator.
     */
    @Test
    public void testOrchestratorRequest() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getQueryWorkflowRequestMsg(DEFAULT_UUID)
        );

        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);
        verify(mOrchestrator).handle(request, mChannelHandlerContext, mServerRouter);
    }

    /**
     * Test that the ManagementServer correctly handles an unknown server error.
     * We verify this by triggering a SerializerException in a BOOTSTRAP_MANAGEMENT
     * request.
     */
    @Test
    public void testLayoutDeserializationException() {
        // Here we do not use the API to facilitate creating a scenario where
        // this exception can occur.
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                RequestPayloadMsg.newBuilder()
                    .setBootstrapManagementRequest(
                            BootstrapManagementRequestMsg.newBuilder()
                                    .setLayout(LayoutMsg.newBuilder().setLayoutJson("INVALID JSON").build())
                                    .build())
                        .build());

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Verify that no Layout was saved.
        verify(mServerContext, never()).saveManagementLayout(any(Layout.class));

        // Assert that the payload has a UNKNOWN_ERROR response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasUnknownError());

        // A SerializerException is expected.
        ByteString bs = response.getPayload().getServerError().getUnknownError().getThrowable();
        try (ObjectInputStream ois = new ObjectInputStream(bs.newInput())) {
            Throwable payloadThrowable = (Throwable) ois.readObject();
            assertTrue(payloadThrowable instanceof SerializerException);
        } catch (Exception ex) {
            fail();
        }
    }

    /**
     * Test that the ManagementServer correctly handles a BOOTSTRAP_MANAGEMENT
     * request. The ManagementServer should not bootstrap with the provided
     * Layout if the clusterId is null, as can be the case with a legacy Layout.
     */
    @Test
    public void testBootstrapManagementNullClusterId() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapManagementRequestMsg(getBasicLayout(1L, null))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mServerContext.getManagementLayout()).thenReturn(null);
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Verify that the Layout was not saved.
        verify(mServerContext, never()).saveManagementLayout(any(Layout.class));

        // Assert that the payload has a BOOTSTRAP_MANAGEMENT response, that the base
        // header fields have remained the same, and that the server was not bootstrapped.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasBootstrapManagementResponse());
        assertFalse(response.getPayload().getBootstrapManagementResponse().getBootstrapped());
    }

    /**
     * Test that the ManagementServer correctly handles a BOOTSTRAP_MANAGEMENT
     * request. The ManagementServer should not bootstrap with the provided
     * Layout if it has already been bootstrapped.
     */
    @Test
    public void testBootstrapManagementAlreadyBootstrapped() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapManagementRequestMsg(getBasicLayout(1L, DEFAULT_UUID))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mServerContext.getManagementLayout()).thenReturn(mock(Layout.class));
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Verify that the Layout was not saved.
        verify(mServerContext, never()).saveManagementLayout(any(Layout.class));

        // Assert that the payload has a BOOTSTRAPPED error response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasBootstrappedError());
    }

    /**
     * Test that the ManagementServer correctly handles a BOOTSTRAP_MANAGEMENT
     * request when it has not been bootstrapped yet.
     */
    @Test
    public void testBootstrapManagement() {
        final Layout layout = getBasicLayout(1L, DEFAULT_UUID);
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapManagementRequestMsg(layout)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mServerContext.getManagementLayout()).thenReturn(null);
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Verify that the Layout was saved.
        verify(mServerContext).saveManagementLayout(eq(layout));

        // Assert that the payload has a BOOTSTRAP_MANAGEMENT response, that the base
        // header fields have remained the same, and that the server was bootstrapped.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasBootstrapManagementResponse());
        assertTrue(response.getPayload().getBootstrapManagementResponse().getBootstrapped());
    }

    /**
     * Test that the ManagementServer correct handles a MANAGEMENT_LAYOUT request
     * when it has not been bootstrapped yet.
     */
    @Test
    public void testManagementLayoutNotBootstrapped() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getManagementLayoutRequestMsg()
        );

        when(mServerContext.getManagementLayout()).thenReturn(null);
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Verify that a NOT_BOOTSTRAPPED error was sent through the router.
        verify(mServerRouter).sendNoBootstrapError(request.getHeader(), mChannelHandlerContext);
    }

    /**
     * Test that the ManagementServer correct handles a MANAGEMENT_LAYOUT request
     * when it has been bootstrapped.
     */
    @Test
    public void testManagementLayout() {
        final Layout layout = getBasicLayout(1L, DEFAULT_UUID);
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getManagementLayoutRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mServerContext.getManagementLayout()).thenReturn(layout);
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a MANAGEMENT_LAYOUT response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasManagementLayoutResponse());

        // Assert that the layout received is as expected.
        assertEquals(layout.asJSONString(),
                response.getPayload().getManagementLayoutResponse().getLayout().getLayoutJson());
    }

    /**
     * Test that the ManagementServer correct handles a QUERY_NODE request. We
     * simulate the node not being bootstrapped, so a default node state is expected.
     */
    @Test
    public void testQueryNode() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getQueryNodeRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mClusterState.getNode(mServerContext.getLocalEndpoint())).thenReturn(Optional.empty());
        managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

        // Assert that the payload has a QUERY_NODE response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasQueryNodeResponse());

        // Assert that the QUERY_NODE response is the default node state.
        QueryNodeResponseMsg msg = response.getPayload().getQueryNodeResponse();
        assertEquals(SequencerStatus.UNKNOWN, msg.getSequencerMetrics().getSequencerStatus());
        assertEquals(NODES.get(0), msg.getNodeConnectivity().getEndpoint());
        assertEquals(0, msg.getNodeConnectivity().getConnectivityInfoCount());
        assertEquals(NodeConnectivityType.NOT_READY, msg.getNodeConnectivity().getConnectivityType());
    }

    /**
     * Test that the ManagementServer correct handles a REPORT_FAILURE request
     * when it has not been bootstrapped yet. A NOT_BOOTSTRAPPED error should
     * be sent by the router with no failure handling taking place.
     */
    @Test
    public void testReportFailureNotBootstrapped() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getReportFailureRequestMsg(1L, new HashSet<>(Collections.singletonList(NODES.get(1))))
        );

        when(mServerContext.getManagementLayout()).thenReturn(null);

        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);
            configMocked.verify(never(), () -> ReconfigurationEventHandler.handleFailure(any(), any(), any(), any()));
        }

        // Verify that a NOT_BOOTSTRAPPED error was sent through the router.
        verify(mServerRouter).sendNoBootstrapError(request.getHeader(), mChannelHandlerContext);
    }

    /**
     * Test that the ManagementServer correct handles a REPORT_FAILURE request
     * when the polling was done in a prior epoch. The request should be discarded
     * and no failure handling should occur.
     */
    @Test
    public void testReportFailureOldPolling() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getReportFailureRequestMsg(0L, new HashSet<>(Collections.singletonList(NODES.get(1))))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        Layout layout = getBasicLayout(1L, DEFAULT_UUID);
        when(mServerContext.getManagementLayout()).thenReturn(mock(Layout.class));
        when(mServerContext.copyManagementLayout()).thenReturn(layout);

        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);
            configMocked.verify(never(), () -> ReconfigurationEventHandler.handleFailure(any(), any(), any(), any()));
        }

        // Assert that the payload has a REPORT_FAILURE response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasReportFailureResponse());
        assertFalse(response.getPayload().getReportFailureResponse().getHandlingSuccessful());
    }

    /**
     * Test that the ManagementServer correct handles a REPORT_FAILURE request when
     * the polling is done in the current epoch.
     */
    @Test
    public void testReportFailure() {
        final String UNRESPONSIVE_NODE = "localhost:9003";
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getReportFailureRequestMsg(1L, new HashSet<>(Arrays.asList(NODES.get(1), UNRESPONSIVE_NODE)))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        Layout layout = getBasicLayout(NODES, NODES, NODES,
                Collections.singletonList(UNRESPONSIVE_NODE), 1L, DEFAULT_UUID);

        when(mServerContext.getManagementLayout()).thenReturn(layout);
        when(mServerContext.copyManagementLayout()).thenReturn(layout);

        // First consider the case when the failure handling is not successful.
        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            HashSet<String> expectedResponsiveFailedNodes = new HashSet<>(Collections.singletonList(NODES.get(1)));
            configMocked.when(() -> ReconfigurationEventHandler.handleFailure(
                    mFailureHandlerPolicy, layout, mCorfuRuntime, expectedResponsiveFailedNodes)).thenReturn(false);

            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

            configMocked.verify(() -> ReconfigurationEventHandler.handleFailure(
                    mFailureHandlerPolicy, layout, mCorfuRuntime, expectedResponsiveFailedNodes));
        }

        // Assert that the payload has a REPORT_FAILURE response and that the base
        // header fields have remained the same. The response should indicate that the
        // handling was not successful.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasReportFailureResponse());
        assertFalse(response.getPayload().getReportFailureResponse().getHandlingSuccessful());

        // Now consider the case when the failure handling is successful.
        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            HashSet<String> expectedResponsiveFailedNodes = new HashSet<>(Collections.singletonList(NODES.get(1)));
            configMocked.when(() -> ReconfigurationEventHandler.handleFailure(
                    mFailureHandlerPolicy, layout, mCorfuRuntime, expectedResponsiveFailedNodes)).thenReturn(true);

            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

            configMocked.verify(() -> ReconfigurationEventHandler.handleFailure(
                    mFailureHandlerPolicy, layout, mCorfuRuntime, expectedResponsiveFailedNodes));
        }

        // Assert that the payload has a REPORT_FAILURE response and that the base header
        // fields have remained the same. The response should indicate that the handling was successful.
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasReportFailureResponse());
        assertTrue(response.getPayload().getReportFailureResponse().getHandlingSuccessful());
    }

    /**
     * Test that the ManagementServer correct handles a HEAL_FAILURE request
     * when it has not been bootstrapped yet. A NOT_BOOTSTRAPPED error should
     * be sent by the router with no healing handling taking place.
     */
    @Test
    public void testHealFailureNotBootstrapped() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHealFailureRequestMsg(1L, new HashSet<>(Collections.singletonList(NODES.get(1))))
        );

        when(mServerContext.getManagementLayout()).thenReturn(null);

        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);
            configMocked.verify(never(), () -> ReconfigurationEventHandler.handleHealing(any(), any(), any()));
        }

        // Verify that a NOT_BOOTSTRAPPED error was sent through the router.
        verify(mServerRouter).sendNoBootstrapError(request.getHeader(), mChannelHandlerContext);
    }

    /**
     * Test that the ManagementServer correct handles a HEAL_FAILURE request
     * when the polling was done in a prior epoch. The request should be discarded
     * and no healing handling should occur.
     */
    @Test
    public void testHealFailureOldPolling() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHealFailureRequestMsg(0L, new HashSet<>(Collections.singletonList(NODES.get(1))))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        Layout layout = getBasicLayout(1L, DEFAULT_UUID);
        when(mServerContext.getManagementLayout()).thenReturn(mock(Layout.class));
        when(mServerContext.copyManagementLayout()).thenReturn(layout);

        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);
            configMocked.verify(never(), () -> ReconfigurationEventHandler.handleHealing(any(), any(), any()));
        }

        // Assert that the payload has a HEAL_FAILURE response and that the base
        // header fields have remained the same.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasHealFailureResponse());
        assertFalse(response.getPayload().getHealFailureResponse().getHandlingSuccessful());
    }

    /**
     * Test that the ManagementServer correct handles a HEAL_FAILURE request when
     * the polling is done in the current epoch.
     */
    @Test
    public void testHealFailure() {
        final String UNRESPONSIVE_NODE = "localhost:9003";
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHealFailureRequestMsg(1L, new HashSet<>(Arrays.asList(NODES.get(1), UNRESPONSIVE_NODE)))
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        Layout layout = getBasicLayout(NODES, NODES, NODES,
                Collections.singletonList(UNRESPONSIVE_NODE), 1L, DEFAULT_UUID);

        when(mServerContext.getManagementLayout()).thenReturn(layout);
        when(mServerContext.copyManagementLayout()).thenReturn(layout);

        // First consider the case when the healing handling is not successful.
        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            HashSet<String> expectedHealedNodes = new HashSet<>(Collections.singletonList(UNRESPONSIVE_NODE));
            configMocked.when(() -> ReconfigurationEventHandler.handleHealing(
                   eq(mCorfuRuntime), eq(expectedHealedNodes), any(Duration.class))).thenReturn(false);

            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

            configMocked.verify(() ->ReconfigurationEventHandler.handleHealing(
                    eq(mCorfuRuntime), eq(expectedHealedNodes), any(Duration.class)));
        }

        // Assert that the payload has a HEAL_FAILURE response and that the base
        // header fields have remained the same. The response should indicate that the
        // handling was not successful.
        verify(mServerRouter).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        ResponseMsg response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasHealFailureResponse());
        assertFalse(response.getPayload().getHealFailureResponse().getHandlingSuccessful());

        // Now consider the case when the healing handling is successful.
        try (MockedStatic<ReconfigurationEventHandler> configMocked = mockStatic(ReconfigurationEventHandler.class)) {
            HashSet<String> expectedHealedNodes = new HashSet<>(Collections.singletonList(UNRESPONSIVE_NODE));
            configMocked.when(() -> ReconfigurationEventHandler.handleHealing(
                    eq(mCorfuRuntime), eq(expectedHealedNodes), any(Duration.class))).thenReturn(true);

            managementServer.handleMessage(request, mChannelHandlerContext, mServerRouter);

            configMocked.verify(() ->ReconfigurationEventHandler.handleHealing(
                    eq(mCorfuRuntime), eq(expectedHealedNodes), any(Duration.class)));
        }

        // Assert that the payload has a HEAL_FAILURE response and that the base header
        // fields have remained the same. The response should indicate that the handling was successful.
        verify(mServerRouter, times(2)).sendResponse(responseCaptor.capture(), eq(mChannelHandlerContext));
        response = responseCaptor.getValue();
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasHealFailureResponse());
        assertTrue(response.getPayload().getHealFailureResponse().getHandlingSuccessful());
    }
}
