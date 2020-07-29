package org.corfudb.common.protocol.client;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * This class contains the tests for all the methods of PeerClient.
 * <p>
 * Created by fchetan on 7/23/20
 */
@RunWith(MockitoJUnitRunner.class)
@Slf4j
class PeerClientTest {

    private final PeerClient spyPeerClient = Mockito.spy(new PeerClient());

    private final UUID clientId = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {

    }

    @Test
    void ping() {
        // Set clientID required for the header of each request object.
        spyPeerClient.setConfig(ClientConfig.builder().clientId(clientId).build());

        // Since there is no server,
        // return true when sendRequest() is called with any Request class.
        // We inspect the Request object passed as an argument to sendRequest() later in each test.
        doReturn(CompletableFuture.completedFuture(true)).
                when(spyPeerClient).sendRequest(any(CorfuProtocol.Request.class));

        // Call the ping method and check its return value.
        try {
            Assertions.assertThat(spyPeerClient.ping().get()).isEqualTo(true);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        }

        verifyRequest(CorfuProtocol.MessageType.PING);
    }


    @Test
    void restart() {
        // Set clientID required for the header of each request object.
        spyPeerClient.setConfig(ClientConfig.builder().clientId(clientId).build());

        // Since there is no server,
        // return true when sendRequest() is called with any Request class.
        // We inspect the Request object passed as an argument to sendRequest() later in each test.
        doReturn(CompletableFuture.completedFuture(true)).
                when(spyPeerClient).sendRequest(any(CorfuProtocol.Request.class));

        // Call the restart method and check its return value.
        try {
            Assertions.assertThat(spyPeerClient.restart().get()).isEqualTo(true);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        }

        verifyRequest(CorfuProtocol.MessageType.RESTART);
    }

    @Test
    void reset() {
        // Set clientID required for the header of each request object.
        spyPeerClient.setConfig(ClientConfig.builder().clientId(clientId).build());

        // Since there is no server,
        // return true when sendRequest() is called with any Request class.
        // We inspect the Request object passed as an argument to sendRequest() later in each test.
        doReturn(CompletableFuture.completedFuture(true)).
                when(spyPeerClient).sendRequest(any(CorfuProtocol.Request.class));

        // Call the reset method and check its return value.
        try {
            Assertions.assertThat(spyPeerClient.reset().get()).isEqualTo(true);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        }

        verifyRequest(CorfuProtocol.MessageType.RESET);
    }

    @Test
    void sealRemoteServer() {
        // Set clientID required for the header of each request object.
        spyPeerClient.setConfig(ClientConfig.builder().clientId(clientId).build());

        // Since there is no server,
        // return true when sendRequest() is called with any Request class.
        // We inspect the Request object passed as an argument to sendRequest() later in each test.
        doReturn(CompletableFuture.completedFuture(true)).
                when(spyPeerClient).sendRequest(any(CorfuProtocol.Request.class));

        // Call the sealRemoteServer method and check its return value.
        try {
            long epoch = new Random().nextLong();
            Assertions.assertThat(spyPeerClient.sealRemoteServer(epoch).get()).isEqualTo(true);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
        }

        verifyRequest(CorfuProtocol.MessageType.SEAL);
    }

    private void verifyRequest(CorfuProtocol.MessageType messageType) {
        // Check that sendRequest() was invoked once and get (capture) its argument.
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        verify(spyPeerClient, times(1))
                .sendRequest(requestArgumentCaptor.capture());
        Request spyPeerClientRequest = requestArgumentCaptor.getAllValues().get(0);

        // (Optional) Check that the request contained the Client ID set initially.
        Assertions.assertThat(spyPeerClientRequest.getHeader().getClientId())
                .isEqualTo(API.getUUID(clientId));

        // Check the payload type of Request that was was passed as an argument to sendRequest().
        switch (messageType) {
            case PING:
                Assertions.assertThat(spyPeerClientRequest.hasPingRequest()).isTrue();
                break;
            case RESTART:
                Assertions.assertThat(spyPeerClientRequest.hasRestartRequest()).isTrue();
                break;
            case RESET:
                Assertions.assertThat(spyPeerClientRequest.hasResetRequest()).isTrue();
                break;
            case SEAL:
                Assertions.assertThat(spyPeerClientRequest.hasSealRequest()).isTrue();
                break;
        }
    }


}