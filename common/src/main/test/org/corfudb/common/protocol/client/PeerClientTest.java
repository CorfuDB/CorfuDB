package org.corfudb.common.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.embedded.EmbeddedChannel;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * This class contains the tests for all the methods of PeerClient.
 * <p>
 * Created by fchetan on 7/23/20
 */
@Slf4j
class PeerClientTest {

    private final PeerClient peerClient = new PeerClient();

    private final ChannelHandler channelHandler = new ChannelHandler();

    private final EmbeddedChannel embeddedChannel = new EmbeddedChannel();

    private final UUID clientId = UUID.randomUUID();

    @Test
    void ping() {
        // Since there is no server, the Request objects are written into the embeddedChannel.
        // We inspect these Request objects from the embeddedChannel later in each test.
        channelHandler.setChannel(embeddedChannel);
        peerClient.setChannelHandler(channelHandler);

        // Set the clientID required for the header of each request object.
        peerClient.getChannelHandler().setConfig(ClientConfig.builder().clientId(clientId).build());

        // Call the ping method and check its outbound value in the embeddedChannel.
        Assertions.assertThat(peerClient.ping());

        // Read the out bound message from the embeddedChannel
        ByteBuf outBuf = embeddedChannel.readOutbound();
        verifyRequest(outBuf,CorfuProtocol.MessageType.PING);
        outBuf.release();
    }


    @Test
    void restart() {
        // Since there is no server, the Request objects are written into the embeddedChannel.
        // We inspect these Request objects from the embeddedChannel later in each test.
        channelHandler.setChannel(embeddedChannel);
        peerClient.setChannelHandler(channelHandler);

        // Set the clientID required for the header of each request object.
        peerClient.getChannelHandler().setConfig(ClientConfig.builder().clientId(clientId).build());

        // Call the restart method and check its outbound value in the embeddedChannel.
        Assertions.assertThat(peerClient.restart());

        // Read the out bound message from the embeddedChannel
        ByteBuf outBuf = embeddedChannel.readOutbound();
        verifyRequest(outBuf,CorfuProtocol.MessageType.RESTART);
        outBuf.release();
    }

    @Test
    void reset() {
        // Since there is no server, the Request objects are written into the embeddedChannel.
        // We inspect these Request objects from the embeddedChannel later in each test.
        channelHandler.setChannel(embeddedChannel);
        peerClient.setChannelHandler(channelHandler);

        // Set the clientID required for the header of each request object.
        peerClient.getChannelHandler().setConfig(ClientConfig.builder().clientId(clientId).build());

        // Call the reset method and check its outbound value in the embeddedChannel.
        Assertions.assertThat(peerClient.reset());

        // Read the out bound message from the embeddedChannel
        ByteBuf outBuf = embeddedChannel.readOutbound();
        verifyRequest(outBuf,CorfuProtocol.MessageType.RESET);
        outBuf.release();
    }

    @Test
    void sealRemoteServer() {
        // Since there is no server, the Request objects are written into the embeddedChannel.
        // We inspect these Request objects from the embeddedChannel later in each test.
        channelHandler.setChannel(embeddedChannel);
        peerClient.setChannelHandler(channelHandler);

        // Set the clientID required for the header of each request object.
        peerClient.getChannelHandler().setConfig(ClientConfig.builder().clientId(clientId).build());

        long epoch = new Random().nextLong();

        // Call the seal method and check its outbound value in the embeddedChannel.
        Assertions.assertThat(peerClient.sealRemoteServer(epoch));

        // Read the out bound message from the embeddedChannel
        ByteBuf outBuf = embeddedChannel.readOutbound();
        verifyRequest(outBuf,CorfuProtocol.MessageType.SEAL);
        outBuf.release();
    }

    private void verifyRequest(ByteBuf outBuf, CorfuProtocol.MessageType messageType) {
        outBuf.readByte();
        ByteBufInputStream msgInputStream = new ByteBufInputStream(outBuf);
        Request outBoundRequest;
        try {
            outBoundRequest = Request.parseFrom(msgInputStream);
            CorfuProtocol.Header header = outBoundRequest.getHeader();

            // (Optional) Check that the request contained the Client ID set initially.
            Assertions.assertThat(outBoundRequest.getHeader().getClientId())
                    .isEqualTo(API.getUUID(clientId));

            // Check the payload type of Request that was was passed as an argument to sendRequest().
            switch (messageType) {
                case PING:
                    Assertions.assertThat(outBoundRequest.hasPingRequest()).isTrue();
                    Assertions.assertThat(header.getType()).isEqualTo(CorfuProtocol.MessageType.PING);
                    break;
                case RESTART:
                    Assertions.assertThat(outBoundRequest.hasRestartRequest()).isTrue();
                    Assertions.assertThat(header.getType()).isEqualTo(CorfuProtocol.MessageType.RESTART);
                    break;
                case RESET:
                    Assertions.assertThat(outBoundRequest.hasResetRequest()).isTrue();
                    Assertions.assertThat(header.getType()).isEqualTo(CorfuProtocol.MessageType.RESET);
                    break;
                case SEAL:
                    Assertions.assertThat(outBoundRequest.hasSealRequest()).isTrue();
                    Assertions.assertThat(header.getType()).isEqualTo(CorfuProtocol.MessageType.SEAL);
                    break;
            }
            msgInputStream.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}