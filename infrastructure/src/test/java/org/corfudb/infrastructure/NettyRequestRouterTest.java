package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class NettyRequestRouterTest {

    private ServerContext serverContext;
    private ImmutableMap<Class, AbstractServer> servers;
    private NettyRequestRouter requestRouter;
    private EmbeddedChannel channel;

    private Random generator = new Random();
    private UUID clusterId;
    private UUID clientId;

    /**
     * Utility method that prepares a ByteBuf containing the incoming Request
     * to be received, marking the first byte with the desired mark.
     */
    private void prepareRequest(ByteBuf byteBuf, Request request, byte mark) {
        ByteBufOutputStream requestOutputStream = new ByteBufOutputStream(byteBuf);

        try {
            requestOutputStream.writeByte(mark);
            request.writeTo(requestOutputStream);
        } catch(IOException ex) {
            log.warn("prepareRequest[{}]: IOException occurred {}",
                    request.getHeader().getRequestId(), ex.toString());
        } finally {
            IOUtils.closeQuietly(requestOutputStream);
        }
    }

    /**
     * Utility method that obtains a Response from a ByteBuf,
     * asserting that the first byte has the desired mark.
     */
    private Response parseResponse(ByteBuf byteBuf, byte mark) {
        ByteBufInputStream responseInputStream = new ByteBufInputStream(byteBuf);
        Response response = null;

        try {
            assertTrue(responseInputStream.readByte() == mark);
            response = Response.parseFrom(responseInputStream);
        } catch(IOException ex) {
            log.warn("parseResponse: IOException occurred: " + ex.toString());
        } finally {
            IOUtils.closeQuietly(responseInputStream);
        }

        return response;
    }

    private boolean compareCommonHeaderFields(Header requestHeader, Header responseHeader) {
        return requestHeader.getRequestId() == responseHeader.getRequestId() &&
                requestHeader.getPriority().equals(responseHeader.getPriority()) &&
                requestHeader.getType().equals(responseHeader.getType()) &&
                requestHeader.getEpoch() == responseHeader.getEpoch() &&
                requestHeader.getClientId().equals(responseHeader.getClientId()) &&
                requestHeader.getClusterId().equals(responseHeader.getClusterId());
    }

    @Before
    public void setup() {
        // Generate a random clusterId and clientId used in the header of requests received.
        clusterId = new UUID(generator.nextLong(), generator.nextLong());
        clientId = new UUID(generator.nextLong(), generator.nextLong());

        // Mock the Servers, ServerContext and RequestHandlerMethods.
        serverContext = mock(ServerContext.class);
        servers = ImmutableMap.<Class, AbstractServer>builder()
                .put(BaseServer.class, mock(BaseServer.class))
                .build();

        RequestHandlerMethods baseHandlerMethods = mock(RequestHandlerMethods.class);

        when(servers.get(BaseServer.class).getHandlerMethods()).thenReturn(baseHandlerMethods);
        when(baseHandlerMethods.getHandledTypes()).thenReturn(ImmutableSet.<MessageType>builder().add(MessageType.PING).build());

        requestRouter = new NettyRequestRouter(servers.values().asList(), serverContext);
        channel = new EmbeddedChannel(requestRouter);
    }

    /**
     * Test that sends a message marked with LEGACY_CORFU_MSG_MARK.
     * The message should remain unchanged and be propagated to the next handler.
     * This test can be removed once the wire format is Protobuf only.
     */
    @Test
    public void testLegacyMarkedMessage() {
        final int numBytes = 256;

        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer();
        input.writeByte(API.LEGACY_CORFU_MSG_MARK);

        // Note: message content unimportant
        for(int i = 0; i < numBytes; i++) {
            input.writeByte((byte)i);
        }

        channel.writeInbound(input);
        ByteBuf propagatedMsg = channel.readInbound();
        assertNull(channel.readOutbound());
        assertNull(channel.readInbound());
        assertTrue(channel.isOpen());
        channel.finish();

        byte b = propagatedMsg.readByte();
        assertTrue(b == API.LEGACY_CORFU_MSG_MARK);

        for(int i = 0; i < numBytes; i++) {
            b = propagatedMsg.readByte();
            assertTrue(b == (byte)i);
        }

        assertFalse(propagatedMsg.isReadable());
    }

    /**
     * Test that sends a message with an invalid mark.
     * An IllegalStateException should be thrown and caught, and the channel closed.
     * This test can be removed once the wire format is Protobuf only.
     */
    @Test
    public void testCorruptMarkedRequest() {
        final byte corruptMark = 0xF;

        Header header = API.getHeader(generator.nextLong(), CorfuProtocol.Priority.NORMAL,
                MessageType.PING, 0, clusterId, clientId, false, true);

        Request request = API.getPingRequest(header);
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer();
        prepareRequest(input, request, corruptMark);

        channel.writeInbound(input);

        assertNull(channel.readInbound());
        assertNull(channel.readOutbound());
        assertFalse(channel.isOpen());
    }

    /**
     * Test that sends a request, before bootstrap is complete.
     * A NOT_BOOTSTRAPPED error should be received.
     */
    @Test
    public void testNotBootstrapped() {
        Header header = API.getHeader(generator.nextLong(), CorfuProtocol.Priority.NORMAL,
                MessageType.PING, 0, clusterId, clientId, false, true);

        Request request = API.getPingRequest(header);
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer();
        prepareRequest(input, request, API.PROTO_CORFU_MSG_MARK);

        when(serverContext.getCurrentLayout()).thenReturn(null);

        channel.writeInbound(input);

        ByteBuf output = channel.readOutbound();
        Response response = parseResponse(output, API.PROTO_CORFU_MSG_MARK);
        assertFalse(output.isReadable());
        output.release();

        assertNull(channel.readInbound());
        assertTrue(channel.isOpen());
        channel.finish();

        assertTrue(compareCommonHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getError().getCode().equals(CorfuProtocol.ERROR.NOT_BOOTSTRAPPED));
    }

    /**
     * Test that sends a request with an incorrect cluster id, after bootstrap.
     * A WRONG_CLUSTER error should be received containing the cluster id that
     * received the request, along with the cluster id sent by the client.
     */
    @Test
    public void testInvalidClusterIdRequest() {
        final UUID badClusterId = new UUID(clusterId.getMostSignificantBits() + 1, clusterId.getLeastSignificantBits());
        final Layout layout = mock(Layout.class);

        Header headerBadClusterId = API.getHeader(generator.nextLong(), CorfuProtocol.Priority.NORMAL,
                MessageType.PING, 0, badClusterId, clientId, false, true);

        Request request = API.getPingRequest(headerBadClusterId);
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer();
        prepareRequest(input, request, API.PROTO_CORFU_MSG_MARK);

        when(serverContext.getCurrentLayout()).thenReturn(layout);
        when(layout.getClusterId()).thenReturn(clusterId);

        channel.writeInbound(input);
        ByteBuf output = channel.readOutbound();
        Response response = parseResponse(output, API.PROTO_CORFU_MSG_MARK);
        assertFalse(output.isReadable());
        output.release();

        assertNull(channel.readInbound());
        assertTrue(channel.isOpen());
        channel.finish();

        assertTrue(compareCommonHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getError().getCode().equals(CorfuProtocol.ERROR.WRONG_CLUSTER));
        assertTrue(response.getError().getWrongClusterPayload().getClientClusterId().equals(API.getUUID(badClusterId)));
        assertTrue(response.getError().getWrongClusterPayload().getServerClusterId().equals(API.getUUID(clusterId)));
    }

    /**
     * Test that sends a request with a bad epoch.
     * A WRONG_EPOCH error should be received containing the current epoch.
     */
    @Test
    public void testInvalidEpochRequest() {
        final long currentEpoch = requestRouter.getServerEpoch();
        final long badEpoch = currentEpoch - 1;

        Header headerBadEpoch = API.getHeader(generator.nextLong(), CorfuProtocol.Priority.NORMAL,
                MessageType.PING, badEpoch, clusterId, clientId, false, false);

        Request request = API.getPingRequest(headerBadEpoch);
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer();
        prepareRequest(input, request, API.PROTO_CORFU_MSG_MARK);

        channel.writeInbound(input);
        ByteBuf output = channel.readOutbound();
        Response response = parseResponse(output, API.PROTO_CORFU_MSG_MARK);
        assertFalse(output.isReadable());
        output.release();

        assertNull(channel.readInbound());
        assertTrue(channel.isOpen());
        channel.finish();

        assertTrue(compareCommonHeaderFields(request.getHeader(), response.getHeader()));
        assertTrue(response.getError().getCode().equals(CorfuProtocol.ERROR.WRONG_EPOCH));
        assertTrue(response.getError().getWrongEpochPayload() == currentEpoch);
    }
}
