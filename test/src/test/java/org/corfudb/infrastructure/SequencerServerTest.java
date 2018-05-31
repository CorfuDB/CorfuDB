package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/13/15.
 */
public class SequencerServerTest extends AbstractServerTest {

    public SequencerServerTest() {
        super();
    }

    SequencerServer server;

    @Override
    public AbstractServer getDefaultServer() {
        server = new SequencerServer(ServerContextBuilder.emptyContext());
        return server;
    }

    @Before
    public void bootstrapSequencer() {
        server.setBootstrapEpoch(0L);
    }

    /**
     * Verifies that the SEQUENCER_METRICS_REQUEST is responded by the SEQUENCER_METRICS_RESPONSE
     */
    @Test
    public void sequencerMetricsRequest() {
        sendMessage(CorfuMsgType.SEQUENCER_METRICS_REQUEST.msg());
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.SEQUENCER_METRICS_RESPONSE);
    }

    @Test
    public void responseForEachRequest() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.emptyList())));
            assertThat(getResponseMessages().size())
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void tokensAreIncreasing() {
        long lastTokenValue = -1;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.emptyList())));
            Token thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();
            assertThat(thisToken.getTokenValue())
                    .isGreaterThan(lastTokenValue);
            lastTokenValue = thisToken.getTokenValue();
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.emptyList())));
            Token thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.emptyList())));
            Token checkToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisToken)
                    .isEqualTo(checkToken);
        }
    }

    @Test
    public void perStreamCheckTokenPositionWorks() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            Token thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamA))));
            Token checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            Token thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamB))));
            Token checkTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamA))));
            Token checkTokenA2 = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(checkTokenA2)
                    .isEqualTo(checkTokenA);

            assertThat(thisTokenB.getTokenValue())
                    .isGreaterThan(checkTokenA2.getTokenValue());
        }
    }

    @Test
    public void checkBackpointersWork() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            Token thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            long checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue())
                    .isEqualTo(checkTokenAValue);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            Token thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            long checkTokenBValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamB);

            assertThat(thisTokenB.getTokenValue())
                    .isEqualTo(checkTokenBValue);

            final long MULTI_TOKEN = 5L;

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue() + MULTI_TOKEN - 1)
                    .isEqualTo(checkTokenAValue);

            // check the requesting multiple tokens does not break the back-pointer for the multi-entry
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue())
                    .isEqualTo(checkTokenAValue);

        }
    }

    @Test
    public void SequencerWillResetTails() throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        UUID streamC = UUID.nameUUIDFromBytes("streamC".getBytes());

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamA))));
        long tailA = getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamB))));
        long tailB = getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));

        long tailC = getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.emptyList())));
        long globalTail = getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue();

        // Construct new tails
        Map<UUID, Long> tailMap = new HashMap<>();
        long newTailA = tailA + 2;
        long newTailB = tailB + 1;
        // This one should not be updated
        long newTailC = tailC - 1;

        tailMap.put(streamA, newTailA);
        tailMap.put(streamB, newTailB);
        tailMap.put(streamC, newTailC);

        // Modifying the bootstrapEpoch to simulate sequencer reset.
        server.setBootstrapEpoch(-1L);
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.BOOTSTRAP_SEQUENCER,
                new SequencerTailsRecoveryMsg(globalTail + 2, tailMap, 0L, false)));

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamA))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue()).isEqualTo(newTailA);

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamB))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue()).isEqualTo(newTailB);

        // We should have the same value than before
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamC))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getTokenValue()).isEqualTo(newTailC);
    }
}
