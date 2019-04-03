package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.runtime.view.Address;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mwei on 12/13/15.
 */
public class SequencerServerTest extends AbstractServerTest {

    public SequencerServerTest() {
        super();
    }

    ServerContext serverContext;

    SequencerServer server;

    @Override
    public AbstractServer getDefaultServer() {
        serverContext = ServerContextBuilder.defaultTestContext(SERVERS.PORT_0);
        server = new SequencerServer(serverContext);
        return server;
    }

    @Before
    public void bootstrapSequencer() {
        server.setSequencerEpoch(0L);
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
            assertThat(thisToken.getSequence())
                    .isGreaterThan(lastTokenValue);
            lastTokenValue = thisToken.getSequence();
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

            assertThat(thisTokenB.getSequence())
                    .isGreaterThan(checkTokenA2.getSequence());
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

            assertThat(thisTokenA.getSequence())
                    .isEqualTo(checkTokenAValue);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            Token thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            long checkTokenBValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamB);

            assertThat(thisTokenB.getSequence())
                    .isEqualTo(checkTokenBValue);

            final long MULTI_TOKEN = 5L;

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getSequence() + MULTI_TOKEN - 1)
                    .isEqualTo(checkTokenAValue);

            // check the requesting multiple tokens does not break the back-pointer for the multi-entry
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getSequence())
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
        long tailA = getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamB))));
        long tailB = getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));

        long tailC = getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence();

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.emptyList())));
        long globalTail = getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence();

        // Construct new tails
        Map<UUID, Long> tailMap = new HashMap<>();
        long newTailA = tailA + 2;
        long newTailB = tailB + 1;
        // This one should not be updated
        long newTailC = tailC - 1;

        tailMap.put(streamA, newTailA);
        tailMap.put(streamB, newTailB);
        tailMap.put(streamC, newTailC);

        // Modifying the sequencerEpoch to simulate sequencer reset.
        server.setSequencerEpoch(-1L);
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.BOOTSTRAP_SEQUENCER,
                new SequencerTailsRecoveryMsg(globalTail + 2, tailMap, 0L, false)));

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamA))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence()).isEqualTo(newTailA);

        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamB))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence()).isEqualTo(newTailB);

        // We should have the same value than before
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamC))));
        assertThat(getLastPayloadMessageAs(TokenResponse.class).getToken().getSequence()).isEqualTo(newTailC);
    }


    /**
     * Scenario to verify that we do not regress the token count when the layout switches primary
     * sequencers.
     * We assert that the failover sequencer should always receive a full bootstrap message rather
     * than an empty bootstrap message (without streamTailsMap)
     */
    @Test
    public void failoverSeqDoesNotRegressTokenValue() {

        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());

        // Request tokens.
        final long num = 10;
        // 0 - 9
        for (int i = 0; i < num; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
        }
        assertThat(server.getGlobalLogTail()).isEqualTo(num);

        // Sequencer accepts a delta bootstrap message only if the new epoch is consecutive.
        long newEpoch = serverContext.getServerEpoch() + 1;
        serverContext.setServerEpoch(newEpoch, serverContext.getServerRouter());
        sendMessage(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerTailsRecoveryMsg(
                Address.NON_EXIST, Collections.emptyMap(), newEpoch, true)));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        // Sequencer accepts only a full bootstrap message if the epoch is not consecutive.
        newEpoch = serverContext.getServerEpoch() + 2;
        serverContext.setServerEpoch(newEpoch, serverContext.getServerRouter());
        sendMessage(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerTailsRecoveryMsg(
                Address.NON_EXIST, Collections.emptyMap(), newEpoch, true)));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.NACK);
        sendMessage(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerTailsRecoveryMsg(
                num, Collections.singletonMap(streamA, num), newEpoch, false)));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);

        sendMessage(CorfuMsgType.TOKEN_REQ.payloadMsg(new TokenRequest(0L, Collections.emptyList())));
        assertThat(getLastPayloadMessageAs(TokenResponse.class))
                .isEqualTo(new TokenResponse(TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY,
                        TokenResponse.NO_CONFLICT_STREAM, new Token(newEpoch, num - 1),
                        Collections.emptyMap(), Collections.emptyList()));
    }

}
