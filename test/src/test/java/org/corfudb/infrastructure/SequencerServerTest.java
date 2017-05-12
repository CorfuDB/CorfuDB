package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.*;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/13/15.
 */
public class SequencerServerTest extends AbstractServerTest {

    public SequencerServerTest() {
        super();
    }

    @Override
    public AbstractServer getDefaultServer() {
        return new
                SequencerServer(ServerContextBuilder.emptyContext());
    }

    @Test
    public void responseForEachRequest() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            assertThat(getResponseMessages().size())
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void tokensAreIncreasing() {
        long lastTokenValue = -1;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            Token thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();
            assertThat(thisToken.getTokenValue())
                    .isGreaterThan(lastTokenValue);
            lastTokenValue = thisToken.getTokenValue();
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet())));
            Token thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.<UUID>emptySet())));
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
                    new TokenRequest(1L, Collections.singleton(streamA))));
            Token thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA))));
            Token checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB))));
            Token thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamB))));
            Token checkTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA))));
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
                    new TokenRequest(1L, Collections.singleton(streamA))));
            Token thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA))));
            long checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue())
                    .isEqualTo(checkTokenAValue);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB))));
            Token thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB))));
            long checkTokenBValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamB);

            assertThat(thisTokenB.getTokenValue())
                    .isEqualTo(checkTokenBValue);

            final long MULTI_TOKEN = 5L;

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singleton(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue() + MULTI_TOKEN - 1)
                    .isEqualTo(checkTokenAValue);

            // check the requesting multiple tokens does not break the back-pointer for the multi-entry
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA))));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singleton(streamA))));
            checkTokenAValue = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getTokenValue())
                    .isEqualTo(checkTokenAValue);

        }
    }

}
