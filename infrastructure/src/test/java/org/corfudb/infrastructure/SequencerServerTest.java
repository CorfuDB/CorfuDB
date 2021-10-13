package org.corfudb.infrastructure;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Sequencer.SequencerMetricsResponseMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamAddressSpace;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getAllStreamsAddressRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getDefaultSequencerMetricsRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerTrimRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getStreamsAddressRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenResponse;
import static org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg.SequencerStatus;
import static org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerRequestMsg;
import static org.corfudb.runtime.proto.service.Sequencer.BootstrapSequencerResponseMsg;
import static org.corfudb.runtime.proto.service.Sequencer.StreamsAddressRequestMsg;
import static org.corfudb.runtime.proto.service.Sequencer.TokenRequestMsg;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class has test methods that test the RPC calls present in {@link SequencerServer}.
 * The RequestMsg type messages that are tested are as follows -
 * - TokenRequestMsg
 * - BootstrapSequencerRequestMsg
 * - SequencerTrimRequestMsg
 * - SequencerMetricsRequestMsg
 * - StreamsAddressRequestMsg
 * See test methods for their functionality descriptions.
 */
@Slf4j
public class SequencerServerTest {

    private final AtomicInteger requestCounter = new AtomicInteger();

    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    // The SequencerServer instance used for testing
    private SequencerServer sequencerServer;

    // Parameters that need to be mocked
    @Mock
    private ServerContext mockServerContext;
    @Mock
    private IServerRouter mockServerRouter;
    @Mock
    private ChannelHandlerContext mockChannelHandlerContext;

    // Using spy as we use the default behaviour most of the times and
    // override only a few times.
    @Spy
    private SequencerServer.SequencerServerInitializer spySequencerFactoryHelper;

    /**
     * A helper method that creates a basic message header populated
     * with default values.
     *
     * @param ignoreClusterId indicates if the message is clusterId aware
     * @param ignoreEpoch     indicates if the message is epoch aware
     * @return the corresponding HeaderMsg
     */
    private HeaderMsg getBasicHeader(ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return getHeaderMsg(requestCounter.incrementAndGet(), PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that compares the base fields of two message headers.
     * These include the request ID, the epoch, the client ID, and the cluster ID.
     *
     * @param requestHeader  the header from the request message
     * @param responseHeader the header from the response message
     * @return true if the two headers have the same base field values
     */
    private boolean compareBaseHeaderFields(HeaderMsg requestHeader, HeaderMsg responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    /**
     * Initialize the DirectExecutorService before running individual tests.
     */
    @Before
    public void setup() {
        when(mockServerContext.getExecutorService(anyInt(), anyString()))
                .thenReturn(MoreExecutors.newDirectExecutorService());
    }

    /**
     * Test that the SequencerServer sends NotReadyErrorMsg when client sends a TokenRequestMsg
     * without bootstrapping it before.
     * When ServerContext epoch and sequencer epoch are not equal, it expects the bootstrap
     * message first.
     * Essentially tests the {@link SequencerServer#isServerReadyToHandleMsg(RequestMsg)} method.
     */
    @Test
    public void testSequencerServerNotReady() {
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        // Make sequencer server's epoch 0L
        sequencerServer.setSequencerEpoch(0L);
        // ServerContext epoch is set only by the bootstrap request.
        // Make it return the Layout.INVALID_EPOCH (-1).
        when(mockServerContext.getServerEpoch()).thenReturn(Layout.INVALID_EPOCH);

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(0, Collections.emptyList())
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a SERVER_ERROR and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasServerError());
        assertTrue(response.getPayload().getServerError().hasNotReadyError());
    }


    /**
     * Test that if sequencer server's epoch is Layout.INVALID_EPOCH (startup i.e -1) OR
     * if epoch sent by the client is not the consecutive epoch of the sequencerEpoch
     * then the sequencer should not accept bootstrapWithoutTailsUpdate
     * BootstrapSequencerRequestMsg, and return
     * {@link BootstrapSequencerResponseMsg}
     * with isBootstrapped set to False.
     */
    @Test
    public void testRequireFullBootstrap() {
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);

        // CASE 1: the sequencer server's epoch = Layout.INVALID_EPOCH
        // and the epoch sent by the client is the consecutive epoch of the sequencerEpoch
        sequencerServer.setSequencerEpoch(Layout.INVALID_EPOCH);

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerRequestMsg(
                        Collections.emptyMap(),
                        0,
                        0,
                        true)
        );

        // Invoke the handler method
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a BootstrapSequencerResponseMsg message with
        // isBootstrapped set to False and that the base header fields have remained the same
        assertThat(compareBaseHeaderFields(request.getHeader(), response.getHeader())).isTrue();
        assertThat(response.getPayload().hasBootstrapSequencerResponse()).isTrue();
        assertThat(response.getPayload().getBootstrapSequencerResponse().getIsBootstrapped())
                .isFalse();

        // Assert that the sequencerServer's and serverContext's Epoch has not changed
        assertEquals(Layout.INVALID_EPOCH, sequencerServer.getSequencerEpoch());
        verify(mockServerContext, never()).setSequencerEpoch(anyLong());

        // CASE 2: the sequencer server's epoch != Layout.INVALID_EPOCH
        // and the epoch sent by the client is NOT the consecutive epoch of the sequencerEpoch
        sequencerServer.setSequencerEpoch(0L);

        request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerRequestMsg(
                        Collections.emptyMap(),
                        0,
                        2L,
                        true)
        );

        // Invoke the handler method
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter, times(2))
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        response = responseCaptor.getValue();

        // Assert that the payload has a BootstrapSequencerResponseMsg message with
        // isBootstrapped set to False and that the base header fields have remained the same
        assertThat(compareBaseHeaderFields(request.getHeader(), response.getHeader())).isTrue();
        assertThat(response.getPayload().hasBootstrapSequencerResponse()).isTrue();
        assertThat(response.getPayload().getBootstrapSequencerResponse().getIsBootstrapped())
                .isFalse();

        // Assert that the sequencerServer's and serverContext's Epoch has not changed
        assertEquals(0, sequencerServer.getSequencerEpoch());
        verify(mockServerContext, never()).setSequencerEpoch(anyLong());
    }

    /**
     * Test that stale bootstrap requests are discarded.
     * If epoch sent by the client is not greater than the sequencerEpoch then the sequencer
     * should discard the
     * {@link BootstrapSequencerRequestMsg},
     * and return a
     * {@link BootstrapSequencerResponseMsg}
     * with isBootstrapped set to False.
     */
    @Test
    public void testStaleBootstrapRequest() {
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        sequencerServer.setSequencerEpoch(1L);
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerRequestMsg(
                        Collections.emptyMap(),
                        0,
                        0,
                        false)
        );

        when(mockServerContext.getServerEpoch()).thenReturn(1L);

        // Invoke the handler method
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a BootstrapSequencerResponseMsg message with
        // isBootstrapped set to False and that the base header fields have remained the same
        assertThat(compareBaseHeaderFields(request.getHeader(), response.getHeader())).isTrue();
        assertThat(response.getPayload().hasBootstrapSequencerResponse()).isTrue();
        assertThat(response.getPayload().getBootstrapSequencerResponse().getIsBootstrapped())
                .isFalse();

        // Assert that the sequencerServer's and serverContext's Epoch has not changed
        assertEquals(1, sequencerServer.getSequencerEpoch());
        verify(mockServerContext, never()).setSequencerEpoch(anyLong());
    }

    /**
     * Test that the SequencerServer correctly handles a BootstrapSequencerRequestMsg. If the
     * epoch sent by the client is greater than or equal to the current server epoch,
     * and bootstrapWithoutTailsUpdate is set as false, a BootstrapSequencerResponseMsg.ACK message
     * should be received.
     * <p>
     * Further, we request the server with a StreamsAddressRequestMsg and verify that
     * the globalLogTail, trimMark and the streamsAddressMap were correctly updated on the server.
     */
    @Test
    public void testBootStrapSequencer() {
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        // Construct new tails
        Map<UUID, StreamAddressSpace> tailMap = new HashMap<>();

        long bootstrapMsgEpoch = 1;
        long globalTailA = 0;
        long globalTailB = 1;
        StreamAddressSpace streamAddressSpaceA = new StreamAddressSpace(Address.NON_ADDRESS,
                Collections.singleton(globalTailA));
        StreamAddressSpace streamAddressSpaceB = new StreamAddressSpace(Address.NON_ADDRESS,
                Collections.singleton(globalTailB));
        tailMap.put(streamA, streamAddressSpaceA);
        tailMap.put(streamB, streamAddressSpaceB);

        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerRequestMsg(
                        tailMap,
                        globalTailB,
                        bootstrapMsgEpoch,
                        false)
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a BOOTSTRAP_SEQUENCER_RESPONSE and that the base
        // header fields have remained the same
        assertThat(compareBaseHeaderFields(request.getHeader(), response.getHeader())).isTrue();
        assertThat(response.getPayload().hasBootstrapSequencerResponse()).isTrue();
        assertThat(response.getPayload().getBootstrapSequencerResponse().getIsBootstrapped())
                .isTrue();

        // Verify that the mockServerContext.setSequencerEpoch() was invoked with bootstrapMsgEpoch
        verify(mockServerContext).setSequencerEpoch(bootstrapMsgEpoch);

        // Assert that serverContext.setSequencerEpoch() argument is equal to bootstrapMsgEpoch
        // Assert that sequencerEpoch value is updated to bootstrapMsgEpoch
        assertEquals(bootstrapMsgEpoch, sequencerServer.getSequencerEpoch());
        // Assert that globalLogTail value is updated to globalTailB
        assertEquals(globalTailB, sequencerServer.getGlobalLogTail());
        // Assert that epochRangeLowerBound value is updated from its initial value
        // Layout.INVALID_EPOCH
        assertEquals(bootstrapMsgEpoch, sequencerServer.getEpochRangeLowerBound());
    }

    /**
     * Test that the SequencerServer correctly handles a StreamsAddressRequestMsg.
     * <p>
     * If the request type is StreamsAddressRequestMsg.STREAMS then the server should respond with
     * the address map of a particular stream requested by the client.
     * <p>
     * If the request type is StreamsAddressRequestMsg.ALL_STREAMS then the server should respond
     * with the address map of all the streams.
     * <p>
     * If the request type is StreamsAddressRequestMsg.INVALID then the server should throw a new
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testStreamAddressRequest() {
        // Mock the Bootstrap of the Sequencer Server with two streams
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        // Construct new tails
        Map<UUID, StreamAddressSpace> tailMap = new HashMap<>();
        long globalTailA = 0;
        long globalTailB = 1;
        StreamAddressSpace streamAddressSpaceA = new StreamAddressSpace(Address.NON_ADDRESS,
                Collections.singleton(globalTailA));
        StreamAddressSpace streamAddressSpaceB = new StreamAddressSpace(Address.NON_ADDRESS,
                Collections.singleton(globalTailB));
        tailMap.put(streamA, streamAddressSpaceA);
        tailMap.put(streamB, streamAddressSpaceB);
        long bootstrapMsgEpoch = 1;

        when(spySequencerFactoryHelper.getStreamAddressSpaceMap()).thenReturn(tailMap);
        // Global log tail is set to 1 when both streams are loaded
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTailB);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);


        // Case 1: Send StreamsAddressRequestMsg.STREAMS RPC call
        // for streamA with globalTailA as the start point.
        // SequencerServer should send streamAddressSpaceA as the response
        // Note: StreamAddressRange = (end, start]
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getStreamsAddressRequestMsg(
                        Collections.singletonList(
                                new StreamAddressRange(streamA, globalTailA, -1)))
        );

        // Sequencer epoch was set to bootstrapMsgEpoch in BootstrapSequencerRequestMsg was sent.
        when(mockServerContext.getServerEpoch()).thenReturn(bootstrapMsgEpoch);
        sequencerServer.setSequencerEpoch(bootstrapMsgEpoch);

        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter).
                sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));

        ResponseMsg response = responseCaptor.getValue();
        List<UuidToStreamAddressSpacePairMsg> streamAddressSpacePairMsgList =
                response.getPayload().getStreamsAddressResponse().getAddressMapList();
        assertEquals(1, streamAddressSpacePairMsgList.size());
        StreamAddressSpace responseStreamAddressSpace =
                getStreamAddressSpace(streamAddressSpacePairMsgList.get(0).getAddressSpace());

        // Verify the values. Note that global tail is of streamB as it was greater than streamA.
        assertEquals(globalTailB, response.getPayload().getStreamsAddressResponse().getLogTail());
        assertEquals(new StreamAddressSpace(Collections.singleton(globalTailA)), responseStreamAddressSpace);
        assertEquals(Address.NON_ADDRESS, responseStreamAddressSpace.getTrimMark());


        // Case 2: Send StreamsAddressRequestMsg.ALL_STREAMS RPC call.
        // SequencerServer should send all streams and their address spaces as the response
        request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getAllStreamsAddressRequestMsg()
        );

        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        verify(mockServerRouter, times(2))
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        response = responseCaptor.getValue();
        streamAddressSpacePairMsgList =
                response.getPayload().getStreamsAddressResponse().getAddressMapList();
        assertEquals(2, streamAddressSpacePairMsgList.size());

        // We have a list of two items of UUID to StreamAddressSpace
        // Assert that each of them has their corresponding values of
        // the globalLogTail, trimMark and the streamsAddressMap.
        ResponseMsg finalResponse = response;
        streamAddressSpacePairMsgList.forEach(uuidToStreamAddressSpacePairMsg ->
        {
            if (uuidToStreamAddressSpacePairMsg.getStreamUuid().getLsb() ==
                    streamA.getLeastSignificantBits() &&
                    uuidToStreamAddressSpacePairMsg.getStreamUuid().getMsb() ==
                            streamA.getMostSignificantBits()) {
                StreamAddressSpace responseStreamAddressSpaceA =
                        getStreamAddressSpace(uuidToStreamAddressSpacePairMsg.getAddressSpace());
                assertEquals(globalTailB,
                        finalResponse.getPayload().getStreamsAddressResponse().getLogTail());
                assertEquals(new StreamAddressSpace(Collections.singleton(globalTailA)), responseStreamAddressSpaceA);
                assertEquals(Address.NON_ADDRESS, responseStreamAddressSpaceA.getTrimMark());
            } else {
                StreamAddressSpace responseStreamAddressSpaceB =
                        getStreamAddressSpace(uuidToStreamAddressSpacePairMsg.getAddressSpace());
                assertEquals(globalTailB,
                        finalResponse.getPayload().getStreamsAddressResponse().getLogTail());
                assertEquals(new StreamAddressSpace(Collections.singleton(globalTailB)), responseStreamAddressSpaceB);
                assertEquals(Address.NON_ADDRESS, responseStreamAddressSpaceB.getTrimMark());
            }
        });
    }

    /**
     * Test that when client sends the SequencerMetricsRequestMsg, the server responds with
     * SequencerMetricsResponseMsg with SequencerStatus.READY as its content.
     */
    @Test
    public void testSequencerMetricsRequest() {
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        sequencerServer.setSequencerEpoch(0L);
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getDefaultSequencerMetricsRequestMsg()
        );

        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a SequencerMetricsResponseMsg and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasSequencerMetricsResponse());

        // Assert that the SequencerMetricsResponseMsg contains SequencerStatus.READY
        SequencerMetricsResponseMsg sequencerMetricsResponseMsg =
                response.getPayload().getSequencerMetricsResponse();
        assertTrue(sequencerMetricsResponseMsg.hasSequencerMetrics());
        assertEquals(SequencerStatus.READY,
                sequencerMetricsResponseMsg.getSequencerMetrics().getSequencerStatus());
    }

    /**
     * Test that when client sends the SequencerMetricsRequestMsg, the server responds with
     * SequencerMetricsResponseMsg with SequencerStatus.READY as its content.
     */
    @Test
    public void testSequencerTrimRequest() {
        // Mock the Bootstrap of the Sequencer Server with a stream
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        // Construct new tailMaps and create a to BootstrapSequencerRequestMsg
        Map<UUID, StreamAddressSpace> tailMap = new HashMap<>();
        long globalTail = 1;
        StreamAddressSpace streamAddressSpace = mock(StreamAddressSpace.class);
        tailMap.put(streamA, streamAddressSpace);
        long bootstrapMsgEpoch = 1;

        SequencerServerCache cache = mock(SequencerServerCache.class);
        when(spySequencerFactoryHelper.getStreamAddressSpaceMap()).thenReturn(tailMap);
        // The global log tail is set to globalTail when sequencer is bootstrapped with the tailMap
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTail);
        doReturn(cache)
                .when(spySequencerFactoryHelper)
                .getSequencerServerCache(anyInt(), anyLong());
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);

        // Send a request with SequencerTrimRequestMsg
        long trimMark = 0;
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getSequencerTrimRequestMsg(trimMark)
        );

        // Sequencer epoch was set to bootstrapMsgEpoch in the previous RPC call.
        when(mockServerContext.getServerEpoch()).thenReturn(bootstrapMsgEpoch);
        sequencerServer.setSequencerEpoch(bootstrapMsgEpoch);

        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);

        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a SequencerMetricsResponseMsg and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        // Assert that response has a sequencerTrimResponse object
        // (Note: SequencerTrimResponse will not have any body)
        assertTrue(response.getPayload().hasSequencerTrimResponse());
        // Verify that sequencerServerCache was invalidated up to the trimMark
        verify(cache).invalidateUpTo(trimMark);

        // Verify that the streamAddressSpace.trim() was called with trimMark argument.
        verify(streamAddressSpace).trim(trimMark);
    }


    /**
     * Tests the {@link TokenRequestMsg} handler method with
     * {@link TokenRequestMsg.TokenRequestType} = TK_QUERY.
     * TK_QUERY is made with numTokens = 0 and list of streamIDs. When a query request is sent,
     * the SequencerServer returns the information about the tail of the log
     * and/or streams without changing/allocating anything.
     *
     * In this method, we test that when request is sent with specific streams list,
     * the response contains the streamTails for those streams. We verify the correctness of the
     * returned token based on the sequencerEpoch and globalLogTail.
     */
    @Test
    public void testHandleTokenQueryRequestWithNonEmptyStreams() {
        // Mock the Bootstrap of the Sequencer Server with two streams
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        List<UUID> streams = new ArrayList<>();
        streams.add(streamA);
        streams.add(streamB);

        // Construct new tails
        Map<UUID, StreamAddressSpace> tailMap = new HashMap<>();
        long globalTailA = 0;
        long globalTailB = 1;

        StreamAddressSpace streamAddressSpaceA = new StreamAddressSpace(Collections.singleton(globalTailA));
        StreamAddressSpace streamAddressSpaceB = new StreamAddressSpace(Collections.singleton(globalTailB));
        tailMap.put(streamA, streamAddressSpaceA);
        tailMap.put(streamB, streamAddressSpaceB);
        long sequencerEpoch = 1;

        // Send a request with TokenRequestMsg
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(0, streams)
        );
        Map<UUID, Long> streamTailToGlobalTailMap = new HashMap<>();
        streamTailToGlobalTailMap.put(streamA, streamAddressSpaceA.getTail());
        streamTailToGlobalTailMap.put(streamB, streamAddressSpaceB.getTail());

        when(spySequencerFactoryHelper.getStreamTailToGlobalTailMap())
                .thenReturn(streamTailToGlobalTailMap);
        // The global log tail is set to globalTailB when sequencer is bootstrapped with tailMap
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTailB);
        when(mockServerContext.getServerEpoch()).thenReturn(1L);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        sequencerServer.setSequencerEpoch(sequencerEpoch);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a TokenResponse and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTokenResponse());

        TokenResponse tokenResponse = getTokenResponse(response.getPayload().getTokenResponse());
        // Verify the contents of the TokenResponse
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertArrayEquals(TokenResponse.NO_CONFLICT_KEY, tokenResponse.getConflictKey());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());

        // Assert that the returned token was made from sequencerEpoch, globalTailB -1
        assertEquals(sequencerEpoch, tokenResponse.getToken().getEpoch());
        assertEquals(0, tokenResponse.getToken().getSequence());

        // Assert that response contains no backpointerMap
        assertEquals(0, tokenResponse.getBackpointerMap().size());

        // cross check that the streamTailToGlobalTailMap values are returned correctly
        assertEquals(globalTailA, tokenResponse.getStreamTail(streamA).longValue());
        assertEquals(globalTailB, tokenResponse.getStreamTail(streamB).longValue());
    }

    /**
     * Tests the {@link TokenRequestMsg} handler method with
     * {@link TokenRequestMsg.TokenRequestType} = TK_QUERY.
     * TK_QUERY is made with numTokens = 0 and list of streamIDs. When a query request is sent,
     * the SequencerServer returns the information about the tail of the log
     * and/or streams without changing/allocating anything.
     *
     * In this method, we test that when request is sent with empty streams list,
     * the response also contains empty streamTails. We verify the correctness of the returned token
     * based on the sequencerEpoch and globalLogTail
     */
    @Test
    public void testHandleTokenQueryRequestWithEmptyStreams() {
        // Assume that the globalLogTail is set to 1. Later in the response verify that token is
        // sent by the server as per this value.
        long globalTail = 1;
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTail);

        long sequencerEpoch = 1;

        // Send a request with TokenRequestMsg
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(0, Collections.emptyList())
        );

        // Set server epoch to 1 making sequencer server ready for TokenRequest RPC call.
        when(mockServerContext.getServerEpoch()).thenReturn(1L);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        // Set sequencerEpoch. Later in the response verify that token is
        // sent by the server as per this value.
        sequencerServer.setSequencerEpoch(sequencerEpoch);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a TokenResponse and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTokenResponse());

        TokenResponse tokenResponse = getTokenResponse(response.getPayload().getTokenResponse());
        // Verify the contents of the TokenResponse
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertArrayEquals(TokenResponse.NO_CONFLICT_KEY, tokenResponse.getConflictKey());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());

        // Assert that the returned token was made from sequencerEpoch and globalTail - 1
        assertEquals(sequencerEpoch, tokenResponse.getToken().getEpoch());
        assertEquals(0, tokenResponse.getToken().getSequence());

        // Assert that response contains no backpointerMap and streamTails are empty
        assertEquals(0, tokenResponse.getBackpointerMap().size());
        assertEquals(0, tokenResponse.getStreamTailsCount());
    }

    /**
     * Tests the {@link TokenRequestMsg} handler method with
     * {@link TokenRequestMsg.TokenRequestType} = TK_RAW.
     * TK_RAW is made with non-zero numTokens and null or empty list of streamIDs.
     * When a query request is sent, the SequencerServer extends the global log tail and returns
     * the global-log token
     *
     * In this method, we test that when request is sent with non-zero numTokens and null or empty
     * streams list, the response also contains empty streamTails. We verify the correctness of the
     * returned token based on the sequencerEpoch, globalLogTail and numTokens from the request.
     *
     * Essentially tests {@link SequencerServer}'s handleRawToken method, which serves log-tokens
     * for a raw log implementation.
     */
    @Test
    public void testHandleRawTokenRequest() {
        // Assume that the globalLogTail is set to 1. Later in the response verify that token is
        // sent by the server as per this value.
        long globalTail = 1;
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTail);

        long sequencerEpoch = 1;
        long numTokens = 5;
        // Send a request with TokenRequestMsg
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(numTokens, Collections.emptyList())
        );

        // Set server epoch to 1 making sequencer server ready for TokenRequest RPC call.
        when(mockServerContext.getServerEpoch()).thenReturn(1L);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        // Set sequencerEpoch. Later in the response verify that token is
        // sent by the server as per this value.
        sequencerServer.setSequencerEpoch(sequencerEpoch);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a TokenResponse and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTokenResponse());

        TokenResponse tokenResponse = getTokenResponse(response.getPayload().getTokenResponse());
        // Verify the contents of the TokenResponse
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertArrayEquals(TokenResponse.NO_CONFLICT_KEY, tokenResponse.getConflictKey());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());

        // Assert that the returned token was made from sequencerEpoch and globalTail value
        // The global tail points to an open slot, not the last written slot
        assertEquals(sequencerEpoch, tokenResponse.getToken().getEpoch());
        assertEquals(globalTail, tokenResponse.getToken().getSequence());
        // Assert that response contains no backpointerMap and streamTails are empty
        assertEquals(0, tokenResponse.getBackpointerMap().size());
        assertEquals(0, tokenResponse.getStreamTailsCount());

        // Assert that server's globalLogTail as advanced by numTokens
        assertEquals(numTokens + globalTail, sequencerServer.getGlobalLogTail());
    }

    /**
     * Tests the {@link TokenRequestMsg} handler method with
     * {@link TokenRequestMsg.TokenRequestType} = TK_TX. (Token_Transaction)
     * TK_TX is made with numTokens and conflictInfo of type
     * {@link org.corfudb.protocols.wireprotocol.TxResolutionInfo}.
     *
     * In this test case we test that if the transaction may commit,
     * then a normal allocation of log position(s) is pursued.
     *
     * Essentially tests {@link SequencerServer}'s handleTxToken and handleAllocation method.
     */
    @Test
    public void testHandleTxTokenRequestNormal() {
        // Assume that the globalLogTail is set to 1. Later in the response verify that token is
        // sent by the server as per this value.
        long globalTail = 1;
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTail);

        long sequencerEpoch = 1;
        long numTokens = 5;
        // Send a request with TokenRequestMsg
        Token snapshotTimestamp = new Token(sequencerEpoch, globalTail);
        UUID txId = UUID.randomUUID();
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(5, Collections.emptyList(),
                        new TxResolutionInfo(txId, snapshotTimestamp))
        );

        // Set server epoch to 1 making sequencer server ready for TokenRequest RPC call.
        when(mockServerContext.getServerEpoch()).thenReturn(1L);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        // Set sequencerEpoch. Later in the response verify that token is
        // sent by the server as per this value.
        sequencerServer.setSequencerEpoch(sequencerEpoch);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();

        // Assert that the payload has a TokenResponse and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTokenResponse());

        TokenResponse tokenResponse = getTokenResponse(response.getPayload().getTokenResponse());
        // Verify the contents of the TokenResponse
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertArrayEquals(TokenResponse.NO_CONFLICT_KEY, tokenResponse.getConflictKey());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());

        // Assert that the returned token was made from sequencerEpoch and globalTail value
        // The global tail points to an open slot, not the last written slot
        assertEquals(sequencerEpoch, tokenResponse.getToken().getEpoch());
        assertEquals(globalTail, tokenResponse.getToken().getSequence());
        // Assert that response contains no backpointerMap and streamTails are empty
        assertEquals(0, tokenResponse.getBackpointerMap().size());
        assertEquals(0, tokenResponse.getStreamTailsCount());

        // Assert that server's globalLogTail is advanced by numTokens
        assertEquals(numTokens + globalTail, sequencerServer.getGlobalLogTail());
    }

    /**
     * Tests the {@link TokenRequestMsg} handler method with
     * {@link TokenRequestMsg.TokenRequestType} = TK_TX. (Token_Transaction)
     * TK_TX is made with numTokens and conflictInfo of type
     * {@link org.corfudb.protocols.wireprotocol.TxResolutionInfo}.
     *
     * In this test case we test that if the transaction must abort,
     * then a 'error token' containing an Address.ABORTED address is returned.
     *
     * Essentially tests {@link SequencerServer}'s handleTxToken method.
     * Note that this test is NOT a comprehensive coverage of the txnCanCommit() method
     * in SequencerServer.
     */
    @Test
    public void testHandleTxTokenRequestAbort() {
        // Assume that the globalLogTail is set to 1. Later in the response verify that token is
        // sent by the server as per this value.
        long globalTail = 1;
        when(spySequencerFactoryHelper.getGlobalLogTail()).thenReturn(globalTail);

        // Set the epoch of token out of valid range to trigger the abort of transaction.
        long txSnapshotEpoch = -2;
        long sequencerEpoch = 1;
        // Send a request with TokenRequestMsg
        Token snapshotTimestamp = new Token(txSnapshotEpoch, globalTail);
        UUID txId = UUID.randomUUID();
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenRequestMsg(5, Collections.emptyList(),
                        new TxResolutionInfo(txId, snapshotTimestamp))
        );

        // Set server epoch to 1 making sequencer server ready for TokenRequest RPC call.
        when(mockServerContext.getServerEpoch()).thenReturn(1L);
        sequencerServer = new SequencerServer(mockServerContext, spySequencerFactoryHelper);
        // Set sequencerEpoch. Later in the response verify that token is
        // sent by the server as per this value.
        sequencerServer.setSequencerEpoch(sequencerEpoch);
        sequencerServer.handleMessage(request, mockChannelHandlerContext, mockServerRouter);
        // Verify that sendResponse() was called and capture the response object
        ArgumentCaptor<ResponseMsg> responseCaptor = ArgumentCaptor.forClass(ResponseMsg.class);
        verify(mockServerRouter)
                .sendResponse(responseCaptor.capture(), any(ChannelHandlerContext.class));
        ResponseMsg response = responseCaptor.getValue();
        // Assert that the payload has a TokenResponse and that the base
        // header fields have remained the same
        assertTrue(compareBaseHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getPayload().hasTokenResponse());

        TokenResponse tokenResponse = getTokenResponse(response.getPayload().getTokenResponse());
        // Verify the contents of the TokenResponse
        assertEquals(TokenType.TX_ABORT_NEWSEQ, tokenResponse.getRespType());
        assertArrayEquals(TokenResponse.NO_CONFLICT_KEY, tokenResponse.getConflictKey());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());

        // Assert that the returned token was made from sequencerEpoch and NON_ADDRESS(-1)
        // since the transaction should be aborted.
        assertEquals(sequencerEpoch, tokenResponse.getToken().getEpoch());
        assertEquals(-1, tokenResponse.getToken().getSequence());
        // Assert that response contains no backpointerMap and streamTails are empty
        assertEquals(0, tokenResponse.getBackpointerMap().size());
        assertEquals(0, tokenResponse.getStreamTailsCount());

        // Assert that server's globalLogTail is NOT advanced.
        assertEquals(globalTail, sequencerServer.getGlobalLogTail());
    }
}
