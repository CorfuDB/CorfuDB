package org.corfudb.runtime;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.corfudb.common.util.CompatibilityVectorUtils;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler;
import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler.ClientHandshakeEvent;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ProtocolVersionMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getHandshakeResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getPingResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientHandshakeHandlerTest {

    // Netty EmbeddedChannel for unit testing.
    private EmbeddedChannel embeddedChannel;

    // Objects that need to be mocked.
    private ChannelHandlerContext mockChannelContext;

    private ChannelPipeline mockChannelPipeline;

    // ClientHandshakeHandler instance we are testing with.
    private ClientHandshakeHandler clientHandshakeHandler;

    // Some utility variables needed for following test cases.
    private final AtomicInteger requestCounter = new AtomicInteger();

    private static final int DEFAULT_HANDSHAKE_TIMEOUT = 10;

    private static final long FAKE_SERVER_VERSION = 111111111;

    private static final UUID SERVER_NODEID = UUID.randomUUID();

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

    @Before
    public void setup() {
        clientHandshakeHandler = new ClientHandshakeHandler(DEFAULT_UUID,
                SERVER_NODEID, DEFAULT_HANDSHAKE_TIMEOUT);
        embeddedChannel = new EmbeddedChannel(clientHandshakeHandler);
        mockChannelContext = mock(ChannelHandlerContext.class);
        mockChannelPipeline = mock(ChannelPipeline.class);
    }

    @Test
    public void testFireHandshakeSucceeded() throws Exception {
        // Get a HandshakeRequestMsg with specified server node id.
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHandshakeResponseMsg(SERVER_NODEID)
        );

        when(mockChannelContext.pipeline()).thenReturn(mockChannelPipeline);
        when(mockChannelPipeline.remove("readTimeoutHandler")).thenReturn(clientHandshakeHandler);

        clientHandshakeHandler.channelRead(mockChannelContext, response);

        verify(mockChannelContext).fireUserEventTriggered(ClientHandshakeEvent.CONNECTED);
    }

    @Test
    public void testVersionMismatchHandshakeSucceeded() throws Exception {
        // Get a HandshakeResponseMsg whose corfu_source_code_version set in the header is different
        // from that at client side.
        ResponseMsg response = getResponseMsg(
            HeaderMsg.newBuilder()
                .setVersion(
                    ProtocolVersionMsg.newBuilder()
                        .setCorfuSourceCodeVersion(FAKE_SERVER_VERSION)
                        .setCapabilityVector(CompatibilityVectorUtils.getCompatibilityVectors())
                        .build())
                .setRequestId(requestCounter.incrementAndGet())
                .setPriority(PriorityLevel.NORMAL)
                .setEpoch(0L)
                .setClusterId(getUuidMsg(DEFAULT_UUID))
                .setClientId(getUuidMsg(DEFAULT_UUID))
                .setIgnoreClusterId(false)
                .setIgnoreEpoch(true)
                .build(),
            getHandshakeResponseMsg(SERVER_NODEID)
        );

        when(mockChannelContext.pipeline()).thenReturn(mockChannelPipeline);
        when(mockChannelPipeline.remove("readTimeoutHandler")).thenReturn(clientHandshakeHandler);

        clientHandshakeHandler.channelRead(mockChannelContext, response);

        // Currently when versions do not match we do nothing but log warning, so the handshake
        // is supposed to succeed.
        verify(mockChannelContext).fireUserEventTriggered(ClientHandshakeEvent.CONNECTED);
    }

    @Test
    public void testResponseDroppedBeforeHandshake() {
        // Take out the handshake request message upon channelActive.
        Object out = embeddedChannel.readOutbound();
        assertTrue(out instanceof RequestMsg);
        assertTrue(((RequestMsg) out).getPayload().hasHandshakeRequest());
        // Get a ping ResponseMsg
        ResponseMsg response = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPingResponseMsg()
        );

        embeddedChannel.writeInbound(response);

        // Verify that the response was correctly dropped and there is no inbound nor outbound messages.
        assertNull(embeddedChannel.readInbound());
        assertNull(embeddedChannel.readOutbound());
    }

    @Test
    public void testResponsePassedAfterHandshake() {
        // Take out the handshake request message upon channelActive.
        Object out = embeddedChannel.readOutbound();
        assertTrue(out instanceof RequestMsg);
        assertTrue(((RequestMsg) out).getPayload().hasHandshakeRequest());
        // Get a HandshakeRequestMsg with specified server node id.
        ResponseMsg handshakeResponse = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHandshakeResponseMsg(SERVER_NODEID)
        );
        // Get a ping ResponseMsg
        ResponseMsg pingResponse = getResponseMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPingResponseMsg()
        );

        embeddedChannel.writeInbound(handshakeResponse);
        embeddedChannel.writeInbound(pingResponse);

        // Verify that the ping response is passed to next handler.
        Object in = embeddedChannel.readInbound();
        assertEquals(in, pingResponse);
        // Verify that there is no outbound messages.
        assertNull(embeddedChannel.readOutbound());
    }

    @After
    public void close() {
        embeddedChannel.finishAndReleaseAll();
    }
}
