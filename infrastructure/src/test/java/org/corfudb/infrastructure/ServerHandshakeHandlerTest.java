package org.corfudb.infrastructure;

import io.netty.channel.embedded.EmbeddedChannel;
import org.corfudb.common.util.CompatibilityVectorUtils;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
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
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getHandshakeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolBase.getPingRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ServerHandshakeHandlerTest {

    // Netty EmbeddedChannel for unit testing.
    private EmbeddedChannel embeddedChannel;

    // Some utility variables needed for following test cases.
    private final AtomicInteger requestCounter = new AtomicInteger();

    private static final int DEFAULT_HANDSHAKE_TIMEOUT = 10;

    private static final long FAKE_CLIENT_VERSION = 111111111;

    private static final long FAKE_SERVER_VERSION = 101010101;

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
        ServerHandshakeHandler serverHandshakeHandler = new ServerHandshakeHandler(SERVER_NODEID,
                FAKE_SERVER_VERSION, DEFAULT_HANDSHAKE_TIMEOUT);
        embeddedChannel = new EmbeddedChannel(serverHandshakeHandler);
    }

    @Test
    public void testHandshakeSucceed() {
        // Get a HandshakeRequestMsg with specified server node id.
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHandshakeRequestMsg(DEFAULT_UUID, SERVER_NODEID)
        );

        embeddedChannel.writeInbound(request);
        Object out = embeddedChannel.readOutbound();

        // Verify that the handshake is complete and HandshakeResponse is sent back.
        assertTrue(out instanceof ResponseMsg);
        assertEquals(SERVER_NODEID, getUUID(((ResponseMsg) out).getPayload().getHandshakeResponse()
                .getServerId()));
    }

    @Test
    public void testVersionMismatchHandshakeSucceed() {
        // Get a HandshakeRequestMsg whose corfu_source_code_version set in the header is different
        // from that at server side.
        RequestMsg request = getRequestMsg(
            HeaderMsg.newBuilder()
                .setVersion(
                     ProtocolVersionMsg.newBuilder()
                    .setCorfuSourceCodeVersion(FAKE_CLIENT_VERSION)
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
            getHandshakeRequestMsg(DEFAULT_UUID, SERVER_NODEID)
        );

        embeddedChannel.writeInbound(request);
        Object out = embeddedChannel.readOutbound();

        // Verify that the handshake could still complete even if the versions of client and server
        // are different.
        assertTrue(out instanceof ResponseMsg);
        assertEquals(SERVER_NODEID, getUUID(((ResponseMsg) out).getPayload().getHandshakeResponse()
                .getServerId()));
    }

    @Test
    public void testRequestDroppedBeforeHandshake() {
        // Get a ping RequestMsg
        RequestMsg request = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPingRequestMsg()
        );

        embeddedChannel.writeInbound(request);

        // Verify that the request was correctly dropped and there is no inbound nor outbound messages.
        assertNull(embeddedChannel.readInbound());
        assertNull(embeddedChannel.readOutbound());
    }

    @Test
    public void testRequestPassedAfterHandshake() {
        // Get a HandshakeRequestMsg with specified server node id.
        RequestMsg handshakeRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getHandshakeRequestMsg(DEFAULT_UUID, SERVER_NODEID)
        );
        // Get a ping RequestMsg
        RequestMsg pingRequest = getRequestMsg(
                getBasicHeader(ClusterIdCheck.CHECK, EpochCheck.IGNORE),
                getPingRequestMsg()
        );

        embeddedChannel.writeInbound(handshakeRequest);
        embeddedChannel.writeInbound(pingRequest);

        Object in = embeddedChannel.readInbound();
        Object out = embeddedChannel.readOutbound();

        // Verify that the ping request is passed to next handler.
        assertEquals(in, pingRequest);
        // Verify that the handshake is complete and HandshakeResponse is sent back.
        assertTrue(out instanceof ResponseMsg);
        assertEquals(SERVER_NODEID, getUUID(((ResponseMsg) out).getPayload().getHandshakeResponse()
                .getServerId()));
    }

    @After
    public void close() {
        embeddedChannel.finishAndReleaseAll();
    }
}
