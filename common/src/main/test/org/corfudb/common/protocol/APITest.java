package org.corfudb.common.protocol;

import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.common.protocol.proto.CorfuProtocol.MessageType.*;

/**
 * Created by fchetan on 7/23/20
 */
class APITest {

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void getUUID() {
        UUID uuid = UUID.randomUUID();
        long lsb = uuid.getLeastSignificantBits();
        long msb = uuid.getMostSignificantBits();

        CorfuProtocol.UUID protoUuid = API.getUUID(uuid);
        assertThat(protoUuid.getLsb()).isEqualTo(lsb);
        assertThat(protoUuid.getMsb()).isEqualTo(msb);
    }

    @Test
    void newHeader() {
        Random random = new Random();
        long requestID = random.nextLong();
        long epoch = random.nextLong();
        UUID clusterId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        CorfuProtocol.Header header = API.newHeader(
                requestID, CorfuProtocol.Priority.HIGH, CorfuProtocol.MessageType.PING,epoch,
                clusterId, clientId, true, false);
        assertThat(header.getRequestId()).isEqualTo(requestID);
        assertThat(header.getPriority()).isEqualTo(CorfuProtocol.Priority.HIGH);
        assertThat(header.getType()).isEqualTo(PING);
        assertThat(header.getEpoch()).isEqualTo(epoch);
        assertThat(header.getClusterId()).isEqualTo(API.getUUID(clusterId));
        assertThat(header.getClientId()).isEqualTo(API.getUUID(clientId));
        assertThat(header.getIgnoreClusterId()).isTrue();
        assertThat(header.getIgnoreEpoch()).isFalse();
    }

    @Test
    void newPingRequest() {
        CorfuProtocol.Header header = getRandomHeader(PING);
        CorfuProtocol.Request request = API.newPingRequest(header);
        assertThat(request.getHeader()).isEqualTo(header);
        assertThat(request.hasPingRequest()).isTrue();
    }

    @Test
    void newPingResponse() {
    }

    @Test
    void newAuthenticateRequest() {
    }

    @Test
    void newAuthenticateResponse() {
    }

    @Test
    void newGetLayoutRequest() {
    }

    @Test
    void newPrepareLayoutRequest() {
    }

    @Test
    void newQueryStreamRequest() {
    }

    @Test
    void testNewQueryStreamRequest() {
    }

    @Test
    void newRestartRequest() {
        CorfuProtocol.Header header = getRandomHeader(RESTART);
        CorfuProtocol.Request request = API.newPingRequest(header);
        assertThat(request.getHeader()).isEqualTo(header);
        assertThat(request.hasPingRequest()).isTrue();
    }

    @Test
    void newRestartResponse() {
    }

    @Test
    void newResetRequest() {
        CorfuProtocol.Header header = getRandomHeader(RESET);
        CorfuProtocol.Request request = API.newResetRequest(header);
        assertThat(request.getHeader()).isEqualTo(header);
        assertThat(request.hasResetRequest()).isTrue();
    }

    @Test
    void newResetResponse() {
    }

    @Test
    void newSealRequest() {
        CorfuProtocol.Header header = getRandomHeader(SEAL);
        long epoch = new Random().nextLong();
        CorfuProtocol.Request request = API.newSealRequest(header,epoch);
        assertThat(request.getHeader()).isEqualTo(header);
        assertThat(request.hasSealRequest()).isTrue();
        assertThat(request.getSealRequest().getEpoch()).isEqualTo(epoch);
    }

    @Test
    void newSealResponse() {
    }

    private CorfuProtocol.Header getRandomHeader(CorfuProtocol.MessageType messageType){
        Random random = new Random();
        long requestID = random.nextLong();
        long epoch = random.nextLong();
        UUID clusterId = UUID.randomUUID();
        UUID clientId = UUID.randomUUID();
        return API.newHeader(
                requestID, CorfuProtocol.Priority.HIGH, messageType,epoch,
                clusterId, clientId, true, false);
    }
}