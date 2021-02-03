package org.corfudb.infrastructure;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getCommitLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getPrepareLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getProposeLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class LayoutServerTest {

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // LayoutServer instance used for testing
    private LayoutServer layoutServer;

    // Objects that need to be mocked
    private ServerContext mockServerContext;
    private IServerRouter mockServerRouter;
    private ChannelHandlerContext mockChannelHandlerContext;
    private DataStore mockDataStore;

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
        mockDataStore = mock(DataStore.class);

        // Initialize with newDirectExecutorService to execute the server RPC
        // handler methods on the calling thread
        when(mockServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());

        // Initialize mocked DataStore for LayoutServer
        when(mockServerContext.getDataStore()).thenReturn(mockDataStore);

        layoutServer = new LayoutServer(mockServerContext);
    }

    /**
     * Test that the LayoutServer correctly handles a BOOTSTRAP_LAYOUT_REQUEST.
     */
    @Test
    public void testBootstrapLayoutAck() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapLayoutRequestMsg(defaultLayout)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        verify(mockServerContext).setCurrentLayout(defaultLayout);
        verify(mockServerContext).setServerEpoch(defaultLayout.getEpoch(), mockServerRouter);

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a BOOTSTRAP_LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasBootstrapLayoutResponse());
        assertTrue(response.getPayload().getBootstrapLayoutResponse().getBootstrapped());
    }

    /**
     * Test that the LayoutServer correctly handles a BOOTSTRAP_LAYOUT_REQUEST with
     * empty layout in the payload.
     */
    @Test
    public void testBootstrapNullLayout() {
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapLayoutRequestMsg(null)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a BOOTSTRAP_LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasBootstrapLayoutResponse());
        assertFalse(response.getPayload().getBootstrapLayoutResponse().getBootstrapped());
    }

    /**
     * Test that the LayoutServer correctly handles a BOOTSTRAP_LAYOUT_REQUEST
     * with a legacy layout having a null clusterId。
     */
    @Test
    public void testBootstrapLayoutWithNullClusterId() throws IOException {
        Layout l = getDefaultLayout();
        l.setClusterId(null);

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapLayoutRequestMsg(l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a BOOTSTRAP_LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasBootstrapLayoutResponse());
        assertFalse(response.getPayload().getBootstrapLayoutResponse().getBootstrapped());
    }

    /**
     * Test that the LayoutServer correctly sends a BootstrappedErrorMsg.
     */
    @Test
    public void testBootstrappedLayout() throws IOException {
        Layout l = getDefaultLayout();
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getBootstrapLayoutRequestMsg(l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a BOOTSTRAP_LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        // Check that Layout Server handler sends a BootstrappedErrorMsg
        ServerErrorMsg serverErrorMsg = response.getPayload().getServerError();
        assertTrue(serverErrorMsg.hasBootstrappedError());
    }

    /**
     * Test that the LayoutServer correctly handles a LAYOUT_REQUEST.
     */
    @Test
    public void testGetLayoutAck() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        long payloadEpoch = 0L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getLayoutRequestMsg(payloadEpoch)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(defaultLayout);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasLayoutResponse());

        Layout retLayout = CorfuProtocolCommon.getLayout(response.getPayload().getLayoutResponse().getLayout());
        assertLayoutMatch(retLayout);
    }

    /**
     * Test that the LayoutServer correctly handles a LAYOUT_REQUEST when it hasn't been bootstrapped.
     */
    @Test
    public void testGetLayoutNoBootstrap() {
        long payloadEpoch = 0L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getLayoutRequestMsg(payloadEpoch)
        );

        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendNoBootstrapError(request.getHeader(), mockChannelHandlerContext);
    }

    /**
     * Test that the LayoutServer correctly handles a LAYOUT_REQUEST with wrong epoch.
     */
    @Test
    public void testGetLayoutWrongEpoch() throws IOException {
        Layout l = getDefaultLayout();
        long wrongEpoch = 5L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                getLayoutRequestMsg(wrongEpoch)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        // Check that Layout Server handler sends a WrongEpochErrorMsg
        ServerErrorMsg serverErrorMsg = response.getPayload().getServerError();
        assertTrue(serverErrorMsg.hasWrongEpochError());
        assertEquals(serverErrorMsg.getWrongEpochError().getCorrectEpoch(), l.getEpoch());
    }

    /**
     * Test that the LayoutServer correctly handles a PREPARE_LAYOUT_REQUEST.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testPrepareAck() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;
        // when there were no proposed rank before
        long defaultRank = -1L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPrepareLayoutRequestMsg(payloadEpoch, phase1Rank)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        ArgumentCaptor<KvRecord<Rank>> recordCaptor = ArgumentCaptor.forClass(KvRecord.class);
        ArgumentCaptor<Rank> rankCaptor = ArgumentCaptor.forClass(Rank.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore).put(recordCaptor.capture(), rankCaptor.capture());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        KvRecord<Rank> record = recordCaptor.getValue();
        Rank rank = rankCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasPrepareLayoutResponse());
        // Verify the LayoutServer performs setPhase1Rank through the paxosDataStore with expected values
        assertEquals(rank.getRank().longValue(), phase1Rank);
        assertEquals(record.getFullKeyName(), "PHASE_1_" + payloadEpoch + "RANK");
        // There should not be proposed layout before and the highest rank was set to -1
        assertNull(CorfuProtocolCommon.getLayout(response.getPayload().getPrepareLayoutResponse().getLayout()));
        assertEquals(response.getPayload().getPrepareLayoutResponse().getRank(), defaultRank);
        assertTrue(response.getPayload().getPrepareLayoutResponse().getPrepared());
    }

    /**
     * Test that the LayoutServer correctly handles a PREPARE_LAYOUT_REQUEST when it hasn't been bootstrapped.
     */
    @Test
    public void testPrepareLayoutNoBootstrap() {
        long payloadEpoch = 0L;
        long phase1Rank = 5L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPrepareLayoutRequestMsg(payloadEpoch, phase1Rank)
        );

        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendNoBootstrapError(request.getHeader(), mockChannelHandlerContext);
        verify(mockDataStore, never()).put(any(), any());
    }

    /**
     * Test that the LayoutServer correctly rejects a PREPARE_LAYOUT_REQUEST when
     * the PREPARE_LAYOUT_REQUEST rank is less than or equal to the highest phase 1 rank, reject.
     */
    @Test
    public void testPrepareWrongPrepareRank() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;
        long highestPhase1Rank = 10L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPrepareLayoutRequestMsg(payloadEpoch, phase1Rank)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockDataStore.get(argThat(new Phase1Matcher(payloadEpoch))))
                .thenReturn(new Rank(highestPhase1Rank, DEFAULT_UUID));
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore, never()).put(any(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasPrepareLayoutResponse());
        // There should not be proposed layout before and the highest rank was set to -1
        assertNull(CorfuProtocolCommon.getLayout(response.getPayload().getPrepareLayoutResponse().getLayout()));
        // PREPARE_LAYOUT_REQUEST should be rejected and the highest phase 1 rank should be returned.
        assertFalse(response.getPayload().getPrepareLayoutResponse().getPrepared());
        assertEquals(response.getPayload().getPrepareLayoutResponse().getRank(), highestPhase1Rank);
    }

    /**
     * Test that the LayoutServer correctly acknowledges a PROPOSE_LAYOUT_REQUEST.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testProposeAck() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;

        RequestMsg proposeRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, phase1Rank, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        ArgumentCaptor<KvRecord<Phase2Data>> recordCaptor = ArgumentCaptor.forClass(KvRecord.class);
        ArgumentCaptor<Phase2Data> phase2DataCaptor = ArgumentCaptor.forClass(Phase2Data.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockDataStore.get(argThat(new Phase1Matcher(payloadEpoch))))
                .thenReturn(new Rank(phase1Rank, DEFAULT_UUID));
        layoutServer.handleMessage(proposeRequest, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore).put(recordCaptor.capture(), phase2DataCaptor.capture());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg proposeResponse = responseCaptor.getValue();
        KvRecord<Phase2Data> record = recordCaptor.getValue();
        Phase2Data phase2Data = phase2DataCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(proposeRequest.getHeader(), proposeResponse.getHeader()));
        assertTrue(proposeResponse.getPayload().hasProposeLayoutResponse());
        // Verify the LayoutServer performs setPhase2Rank through the paxosDataStore with expected values
        assertEquals(phase2Data.getRank().getRank().longValue(), phase1Rank);
        assertEquals(record.getFullKeyName(), "PHASE_2_" + payloadEpoch + "DATA");
        // The PROPOSE_LAYOUT_REQUEST should succeed and proposed rank should equal to the rank in proposeRequest
        assertTrue(proposeResponse.getPayload().getProposeLayoutResponse().getProposed());
        assertEquals(proposeResponse.getPayload().getProposeLayoutResponse().getRank(), phase1Rank);
    }

    /**
     * Test that the LayoutServer correctly handles a PROPOSE_LAYOUT_REQUEST when it hasn't been bootstrapped.
     */
    @Test
    public void testProposeLayoutNoBootstrap() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, phase1Rank, l)
        );

        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendNoBootstrapError(request.getHeader(), mockChannelHandlerContext);
        verify(mockDataStore, never()).put(any(), any());
    }

    /**
     * Test that the LayoutServer correctly rejects a PROPOSE_LAYOUT_REQUEST when
     * there is not corresponding PREPARE_LAYOUT_REQUEST.
     */
    @Test
    public void testProposeRejectNoPrepare() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;
        // when there were no proposed rank before
        long defaultRank = -1L;

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, phase1Rank, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore, never()).put(any(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasProposeLayoutResponse());
        // Propose request should be rejected because there were no phase 1 rank
        assertFalse(response.getPayload().getProposeLayoutResponse().getProposed());
        assertEquals(response.getPayload().getProposeLayoutResponse().getRank(), defaultRank);
    }

    /**
     * Test that the LayoutServer correctly rejects a PROPOSE_LAYOUT_REQUEST when
     * the Layout epoch is not equal to payload epoch.
     */
    @Test
    public void testProposeWrongEpoch() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;
        long wrongEpoch = 5L;
        l.setEpoch(wrongEpoch);

        RequestMsg proposeRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, phase1Rank, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockDataStore.get(argThat(new Phase1Matcher(payloadEpoch))))
                .thenReturn(new Rank(phase1Rank, DEFAULT_UUID));
        layoutServer.handleMessage(proposeRequest, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore, never()).put(any(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg proposeResponse = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(proposeRequest.getHeader(), proposeResponse.getHeader()));
        assertTrue(proposeResponse.getPayload().hasProposeLayoutResponse());
        // The PROPOSE_LAYOUT_REQUEST should be rejected because the Layout epoch is not equal to payload epoch
        assertFalse(proposeResponse.getPayload().getProposeLayoutResponse().getProposed());
        // The LayoutServer should send back the expected phase1rank
        assertEquals(proposeResponse.getPayload().getProposeLayoutResponse().getRank(), phase1Rank);
    }

    /**
     * Test that the LayoutServer correctly rejects a PROPOSE_LAYOUT_REQUEST when
     * the rank in PROPOSE_LAYOUT_REQUEST is less than or equal to the phase1rank
     */
    @Test
    public void testProposeWrongProposeRank() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;
        long wrongProposeRank = 10L;

        RequestMsg proposeRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, wrongProposeRank, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockDataStore.get(argThat(new Phase1Matcher(payloadEpoch))))
                .thenReturn(new Rank(phase1Rank, DEFAULT_UUID));
        layoutServer.handleMessage(proposeRequest, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore, never()).put(any(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg proposeResponse = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(proposeRequest.getHeader(), proposeResponse.getHeader()));
        assertTrue(proposeResponse.getPayload().hasProposeLayoutResponse());
        // The PROPOSE_LAYOUT_REQUEST should be rejected due to wrong propose rank
        assertFalse(proposeResponse.getPayload().getProposeLayoutResponse().getProposed());
        // The LayoutServer should send back the expected phase1rank
        assertEquals(proposeResponse.getPayload().getProposeLayoutResponse().getRank(), phase1Rank);
    }

    /**
     * Test that the LayoutServer correctly rejects a PROPOSE_LAYOUT_REQUEST when
     * the rank in PROPOSE_LAYOUT_REQUEST is equal to the current phase 2 rank.
     */
    @Test
    public void testProposeDuplicateMessage() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long phase1Rank = 5L;

        RequestMsg proposeRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getProposeLayoutRequestMsg(payloadEpoch, phase1Rank, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockDataStore.get(argThat(new Phase1Matcher(payloadEpoch))))
                .thenReturn(new Rank(phase1Rank, DEFAULT_UUID));
        when(mockDataStore.get(argThat(new Phase2Matcher(payloadEpoch))))
                .thenReturn(new Phase2Data(new Rank(phase1Rank, DEFAULT_UUID), l));
        layoutServer.handleMessage(proposeRequest, mockChannelHandlerContext, mockServerRouter);
        verify(mockDataStore, never()).put(any(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg proposeResponse = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(proposeRequest.getHeader(), proposeResponse.getHeader()));
        assertTrue(proposeResponse.getPayload().hasProposeLayoutResponse());
        // The PROPOSE_LAYOUT_REQUEST should be rejected because the rank
        // in PROPOSE_LAYOUT_REQUEST is equal to the current phase 2 rank.
        assertFalse(proposeResponse.getPayload().getProposeLayoutResponse().getProposed());
    }

    /**
     * Test that the LayoutServer correctly acknowledges a COMMIT_LAYOUT_REQUEST.
     */
    @Test
    public void testCommitAck() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        long payloadEpoch = 0L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getCommitLayoutRequestMsg(false, payloadEpoch, defaultLayout)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(defaultLayout);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerContext).setCurrentLayout(defaultLayout);
        verify(mockServerContext).setServerEpoch(payloadEpoch, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasCommitLayoutResponse());
        assertTrue(response.getPayload().getCommitLayoutResponse().getCommitted());
    }

    /**
     * Test that the LayoutServer correctly handles a COMMIT_LAYOUT_REQUEST when it hasn't been bootstrapped.
     */
    @Test
    public void testCommitNoBootstrap() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        long payloadEpoch = 0L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getCommitLayoutRequestMsg(false, payloadEpoch, defaultLayout)
        );

        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerRouter).sendNoBootstrapError(request.getHeader(), mockChannelHandlerContext);
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());
    }

    /**
     * Test that the LayoutServer correctly rejects a COMMIT_LAYOUT_REQUEST when
     * the payloadEpoch is not equal to serverEpoch.
     */
    @Test
    public void testCommitWrongEpoch() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long serverEpoch = 5L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getCommitLayoutRequestMsg(false, payloadEpoch, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockServerContext.getServerEpoch()).thenReturn(serverEpoch);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        // Check that Layout Server handler sends a WrongEpochErrorMsg
        ServerErrorMsg serverErrorMsg = response.getPayload().getServerError();
        assertTrue(serverErrorMsg.hasWrongEpochError());
        assertEquals(serverErrorMsg.getWrongEpochError().getCorrectEpoch(), serverEpoch);
    }

    /**
     * Test that the LayoutServer correctly acknowledges a COMMIT_LAYOUT_REQUEST that is forced set to true.
     */
    @Test
    public void testForceLayoutAck() throws IOException {
        Layout defaultLayout = getDefaultLayout();
        long payloadEpoch = 0L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getCommitLayoutRequestMsg(true, payloadEpoch, defaultLayout)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(defaultLayout);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerContext).setCurrentLayout(defaultLayout);
        verify(mockServerContext).setServerEpoch(payloadEpoch, mockServerRouter);
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasCommitLayoutResponse());
        assertTrue(response.getPayload().getCommitLayoutResponse().getCommitted());
    }

    /**
     * Test that the LayoutServer correctly rejects a forced COMMIT_LAYOUT_REQUEST when
     * the payloadEpoch is not equal to serverEpoch.
     */
    @Test
    public void testForceLayoutReject() throws IOException {
        Layout l = getDefaultLayout();
        long payloadEpoch = 0L;
        long serverEpoch = 5L;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getCommitLayoutRequestMsg(true, payloadEpoch, l)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);

        when(mockServerContext.getCurrentLayout()).thenReturn(l);
        when(mockServerContext.getServerEpoch()).thenReturn(serverEpoch);
        layoutServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        verify(mockServerContext, never()).setCurrentLayout(any());
        verify(mockServerContext, never()).setServerEpoch(anyLong(), any());
        verify(mockServerRouter).sendResponse(responseCaptor.capture(), eq(mockChannelHandlerContext));

        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a LAYOUT_RESPONSE and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasCommitLayoutResponse());
        assertFalse(response.getPayload().getCommitLayoutResponse().getCommitted());
    }

    /* Helper */

    /*
     * Helper method for getting the default Layout from a json file.
     */
    private Layout getDefaultLayout() throws IOException {
        String JSONDefaultLayout = new String(Files.readAllBytes(
                Paths.get("src/test/resources/JSONLayouts/CorfuServerDefaultLayout.json")));

        return Layout.fromJSONString(JSONDefaultLayout);
    }

    /*
     * Helper ArgumentMatcher class for phase1 KvRecord.
     */
    private final class Phase1Matcher implements ArgumentMatcher<KvRecord<Rank>> {

        private final KvRecord<Rank> record;

        public Phase1Matcher(long payloadEpoch) {
            this.record = KvRecord.of(
                    "PHASE_1", payloadEpoch + "RANK", Rank.class
            );
        }

        @Override
        public boolean matches(KvRecord<Rank> rankKvRecord) {
            if (rankKvRecord != null) {
                return record.getPrefix().equals(rankKvRecord.getPrefix())
                        && record.getKey().equals(rankKvRecord.getKey());
            }
            return false;
        }
    }

    /*
     * Helper ArgumentMatcher class for phase2 KvRecord.
     */
    private final class Phase2Matcher implements ArgumentMatcher<KvRecord<Phase2Data>> {

        private final KvRecord<Phase2Data> record;

        public Phase2Matcher(long payloadEpoch) {
            this.record = KvRecord.of(
                    "PHASE_2", payloadEpoch + "DATA", Phase2Data.class
            );
        }

        @Override
        public boolean matches(KvRecord<Phase2Data> rankKvRecord) {
            if (rankKvRecord != null) {
                return record.getPrefix().equals(rankKvRecord.getPrefix())
                        && record.getKey().equals(rankKvRecord.getKey());
            }
            return false;
        }
    }

    /*
     * Helper method that checks the given layout matches the default layout.
     */
    private void assertLayoutMatch(Layout layout) {
        assertThat(layout.getActiveLayoutServers()).containsExactly("localhost:9000", "localhost:9001", "localhost:9002");
        assertThat(layout.getSequencers()).containsExactly("localhost:9000");
        assertThat(layout.getAllLogServers()).containsExactly("localhost:9002", "localhost:9001", "localhost:9000");
    }
}
