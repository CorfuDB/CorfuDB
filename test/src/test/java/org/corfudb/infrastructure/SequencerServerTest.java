package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.SequencerServerAssertions.assertThat;

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
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet(), false, false)));
            assertThat(getResponseMessages().size())
                    .isEqualTo(i + 1);
        }
    }

    @Test
    public void tokensAreIncreasing() {
        long lastToken = -1;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet(), false, false)));
            long thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();
            assertThat(thisToken)
                    .isGreaterThan(lastToken);
            lastToken = thisToken;
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ, new TokenRequest(1L, Collections.<UUID>emptySet(), false, false)));
            long thisToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.<UUID>emptySet(), false, false)));
            long checkToken = getLastPayloadMessageAs(TokenResponse.class).getToken();

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
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            long thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA), false, false)));
            long checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB), false, false)));
            long thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamB), false, false)));
            long checkTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singleton(streamA), false, false)));
            long checkTokenA2 = getLastPayloadMessageAs(TokenResponse.class).getToken();

            assertThat(checkTokenA2)
                    .isEqualTo(checkTokenA);

            assertThat(thisTokenB)
                    .isGreaterThan(checkTokenA2);
        }
    }

    @Test
    public void checkBackpointersWork() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            long thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            long checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB), false, false)));
            long thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB), false, false)));
            long checkTokenB = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamB);

            assertThat(thisTokenB)
                    .isEqualTo(checkTokenB);

            final long MULTI_TOKEN = 5L;

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singleton(streamA), false, false)));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA + MULTI_TOKEN - 1)
                    .isEqualTo(checkTokenA);

            // check the requesting multiple tokens does not break the back-pointer for the multi-entry
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getToken();

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singleton(streamA), false, false)));
            checkTokenA = getLastPayloadMessageAs(TokenResponse.class).getBackpointerMap().get(streamA);

            assertThat(thisTokenA)
                    .isEqualTo(checkTokenA);

        }
    }

    @Test
    public void localAddressesWork() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        long Alocal = -1L;
        long Blocal = -1L;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamA), false, false)));
            long thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getStreamAddresses().get(streamA);

            Alocal++;
            assertThat(thisTokenA)
                    .isEqualTo(Alocal);

            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singleton(streamB), false, false)));
            long thisTokenB = getLastPayloadMessageAs(TokenResponse.class).getStreamAddresses().get(streamB);

            Blocal++;
            assertThat(thisTokenB)
                    .isEqualTo(Blocal);


            sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(2L, Collections.singleton(streamA), false, false)));
            thisTokenA = getLastPayloadMessageAs(TokenResponse.class).getStreamAddresses().get(streamA);

            Alocal+=2;
            assertThat(thisTokenA)
                    .isEqualTo(Alocal);
        }
    }

    @Test
    public void checkSequencerCheckpointingWorks()
            throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        SequencerServer s1 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(0)
                .setCheckpoint(1)
                .build());

        this.router.reset();
        this.router.addServer(s1);
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")), false, false)));
        sendMessage(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singleton(CorfuRuntime.getStreamID("a")), false, false)));
        assertThat(s1)
                .tokenIsAt(2);
        Thread.sleep(PARAMETERS.TIMEOUT_NORMAL.toMillis());
        s1.shutdown();

        SequencerServer s2 = new SequencerServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setInitialToken(-1)
                .setCheckpoint(1)
                .build());
        this.router.reset();
        this.router.addServer(s2);
        assertThat(s2)
                .tokenIsAt(2);
    }

}
