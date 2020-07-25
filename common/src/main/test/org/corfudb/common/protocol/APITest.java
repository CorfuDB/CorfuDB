package org.corfudb.common.protocol;

import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by fchetan on 7/23/20
 */
class APITest {

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void getUUIDTest() {
        UUID uuid = UUID.randomUUID();
        long lsb = uuid.getLeastSignificantBits();
        long msb = uuid.getMostSignificantBits();

        CorfuProtocol.UUID protoUuid = API.getUUID(uuid);
        assertThat(protoUuid.getLsb()==lsb).isTrue();
        assertThat(protoUuid.getMsb()==msb).isTrue();
    }

    @Test
    void newHeader() {
    }

    @Test
    void newPingRequest() {
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
    }

    @Test
    void newRestartResponse() {
    }

    @Test
    void newResetRequest() {
    }

    @Test
    void newResetResponse() {
    }

    @Test
    void newSealRequest() {
    }

    @Test
    void newSealResponse() {
    }
}