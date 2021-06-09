package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getStreamsAddressResponseMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getBootstrapSequencerResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerMetricsResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getSequencerTrimResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolSequencer.getTokenResponseMsg;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class SequencerHandlerTest {

    // The SequencerHandler instance used for testing
    private SequencerHandler sequencerHandler;

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
        return getHeaderMsg(requestCounter.incrementAndGet(), CorfuMessage.PriorityLevel.NORMAL, 0L,
                getUuidMsg(DEFAULT_UUID), getUuidMsg(DEFAULT_UUID), ignoreClusterId, ignoreEpoch);
    }

    /**
     * A helper method that generates a default UUID to Long map for the
     * arguments of getTokenResponseMsg.
     *
     * @return a default UUID to Long HashMap.
     */
    private Map<UUID, Long> getTokenResponseDefaultMap() {
        Map<UUID, Long> defaultMap = new HashMap<>();
        int numIter = 100;
        for (int i = 0; i < numIter; i++) {
            defaultMap.put(UUID.randomUUID(), (long) i);
        }
        return defaultMap;
    }

    /**
     * A helper method that generates a default UUID to StreamAddressSpace map for the
     * arguments of getStreamsAddressResponseMsg.
     *
     * @return a default UUID to StreamAddressSpace HashMap.
     */
    private Map<UUID, StreamAddressSpace> getDefaultAddressMap() {
        Map<UUID, StreamAddressSpace> defaultMap = new HashMap<>();
        int numIter = 10;
        for (int i = 0; i < numIter; i++) {
            defaultMap.put(UUID.randomUUID(), new StreamAddressSpace());
        }
        return defaultMap;
    }

    /**
     * Perform the required preparation before running individual
     * tests by preparing the mocks.
     */
    @Before
    public void setup() {
        mockClientRouter = mock(IClientRouter.class);
        mockChannelHandlerContext = mock(ChannelHandlerContext.class);
        sequencerHandler = new SequencerHandler();
        sequencerHandler.setRouter(mockClientRouter);
    }

    /**
     * Test that the SequencerHandler correctly handles a TOKEN_RESPONSE with empty maps.
     */
    @Test
    public void testTokenResponseEmptyMap() {
        Token token = new Token(0L, 0L);
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenResponseMsg(
                        TokenType.NORMAL,
                        TokenResponse.NO_CONFLICT_KEY,
                        TokenResponse.NO_CONFLICT_STREAM, token,
                        Collections.emptyMap(),
                        Collections.emptyMap())
        );

        sequencerHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<TokenResponse> captor = ArgumentCaptor.forClass(TokenResponse.class);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), captor.capture());

        TokenResponse tokenResponse = captor.getValue();
        assertEquals(token, tokenResponse.getToken());
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());
        assertEquals(0, tokenResponse.getStreamTailsCount());
        assertArrayEquals(tokenResponse.getConflictKey(), TokenResponse.NO_CONFLICT_KEY);
        assertTrue(tokenResponse.getBackpointerMap().isEmpty());
    }

    /**
     * Test that the SequencerHandler correctly handles a TOKEN_RESPONSE with default maps.
     */
    @Test
    public void testTokenResponseDefaultMap() {
        Token token = new Token(0L, 0L);
        Map<UUID, Long> backPointerMap = getTokenResponseDefaultMap();
        Map<UUID, Long> streamTails = getTokenResponseDefaultMap();
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getTokenResponseMsg(
                        TokenType.NORMAL,
                        TokenResponse.NO_CONFLICT_KEY,
                        TokenResponse.NO_CONFLICT_STREAM, token,
                        backPointerMap,
                        streamTails)
        );

        sequencerHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<TokenResponse> captor = ArgumentCaptor.forClass(TokenResponse.class);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), captor.capture());

        TokenResponse tokenResponse = captor.getValue();
        assertEquals(token, tokenResponse.getToken());
        assertEquals(TokenType.NORMAL, tokenResponse.getRespType());
        assertEquals(TokenResponse.NO_CONFLICT_STREAM, tokenResponse.getConflictStream());
        assertEquals(streamTails.size(), tokenResponse.getStreamTailsCount());
        assertEquals(backPointerMap, tokenResponse.getBackpointerMap());
        assertArrayEquals(tokenResponse.getConflictKey(), TokenResponse.NO_CONFLICT_KEY);
    }

    /**
     * Test that the SequencerHandler correctly handles a BOOTSTRAP_SEQUENCER_RESPONSE.
     */
    @Test
    public void testBootstrapSequencerResponse() {
        ResponseMsg responseAck = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getBootstrapSequencerResponseMsg(true)
        );
        ResponseMsg responseNack = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getBootstrapSequencerResponseMsg(false)
        );

        sequencerHandler.handleMessage(responseAck, mockChannelHandlerContext);
        sequencerHandler.handleMessage(responseNack, mockChannelHandlerContext);
        // Verify that the correct request was completed with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(responseAck.getHeader().getRequestId(), true);
        verify(mockClientRouter).completeRequest(responseNack.getHeader().getRequestId(), false);
    }

    /**
     * Test that the SequencerHandler correctly handles a SEQUENCER_TRIM_RESPONSE.
     */
    @Test
    public void testSequencerTrimResponse() {
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSequencerTrimResponseMsg()
        );

        sequencerHandler.handleMessage(response, mockChannelHandlerContext);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(response.getHeader().getRequestId(), true);
    }

    /**
     * Test that the SequencerHandler correctly handles a SEQUENCER_METRICS_RESPONSE.
     */
    @Test
    public void testSequencerMetricsResponseNormal() {
        SequencerMetrics sequencerMetricsReady = SequencerMetrics.READY;
        SequencerMetrics sequencerMetricsNotReady = SequencerMetrics.NOT_READY;
        SequencerMetrics sequencerMetricsUnknown = SequencerMetrics.UNKNOWN;
        ResponseMsg responseReady = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSequencerMetricsResponseMsg(sequencerMetricsReady)
        );
        ResponseMsg responseNotReady = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSequencerMetricsResponseMsg(sequencerMetricsNotReady)
        );
        ResponseMsg responseUnkown = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getSequencerMetricsResponseMsg(sequencerMetricsUnknown)
        );

        sequencerHandler.handleMessage(responseReady, mockChannelHandlerContext);
        sequencerHandler.handleMessage(responseNotReady, mockChannelHandlerContext);
        sequencerHandler.handleMessage(responseUnkown, mockChannelHandlerContext);
        // Verify that the correct request was completed with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(responseReady.getHeader().getRequestId(), sequencerMetricsReady);
        verify(mockClientRouter).completeRequest(responseNotReady.getHeader().getRequestId(), sequencerMetricsNotReady);
        verify(mockClientRouter).completeRequest(responseUnkown.getHeader().getRequestId(), sequencerMetricsUnknown);
    }

    /**
     * Test that the SequencerHandler correctly handles a STREAMS_ADDRESS_RESPONSE with empty address map.
     */
    @Test
    public void testStreamsAddressResponseEmptyAddressMap() {
        long defaultLogTail = 5L;
        long defaultEpoch = 10L;

        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getStreamsAddressResponseMsg(defaultLogTail, defaultEpoch, Collections.emptyMap())
        );

        sequencerHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<StreamsAddressResponse> captor = ArgumentCaptor.forClass(StreamsAddressResponse.class);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), captor.capture());

        StreamsAddressResponse streamsAddressResponse = captor.getValue();
        assertTrue(streamsAddressResponse.getAddressMap().isEmpty());
        assertEquals(defaultLogTail, streamsAddressResponse.getLogTail());
        assertEquals(defaultEpoch, streamsAddressResponse.getEpoch());
    }

    /**
     * Test that the SequencerHandler correctly handles a STREAMS_ADDRESS_RESPONSE with default address map.
     */
    @Test
    public void testStreamsAddressResponseDefaultAddressMap() {
        long defaultLogTail = 5L;
        long defaultEpoch = 10L;
        Map<UUID, StreamAddressSpace> defaultMap = getDefaultAddressMap();

        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.CHECK),
                getStreamsAddressResponseMsg(defaultLogTail, defaultEpoch, defaultMap)
        );

        sequencerHandler.handleMessage(response, mockChannelHandlerContext);
        ArgumentCaptor<StreamsAddressResponse> captor = ArgumentCaptor.forClass(StreamsAddressResponse.class);
        // Verify that the correct request was completed (once) with the appropriate value,
        // and that we did not complete exceptionally.
        verify(mockClientRouter, never()).completeExceptionally(anyLong(), any(Throwable.class));
        verify(mockClientRouter).completeRequest(eq(response.getHeader().getRequestId()), captor.capture());

        StreamsAddressResponse streamsAddressResponse = captor.getValue();
        assertEquals(defaultLogTail, streamsAddressResponse.getLogTail());
        assertEquals(defaultEpoch, streamsAddressResponse.getEpoch());
        Map<UUID, StreamAddressSpace> retMap = streamsAddressResponse.getAddressMap();
        assertEquals(retMap.size(), defaultMap.size());
        for (UUID id : defaultMap.keySet()) {
            assertEquals(defaultMap.get(id).toString(), retMap.get(id).toString());
        }
    }
}
