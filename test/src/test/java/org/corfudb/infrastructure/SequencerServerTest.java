package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerRecoveryMsg;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.runtime.view.Address;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

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
        ServerContext serverContext = new ServerContextBuilder().setSingle(true).build();
        serverContext.installSingleNodeLayoutIfAbsent();
        serverContext.setServerRouter(router);
        router.setServerContext(serverContext);
        serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(), router);
        server = new SequencerServer(serverContext);
        return server;
    }

    @Override
    public void resetTest() {
        super.resetTest();
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
        CompletableFuture<SequencerMetrics> cFuture = sendRequest(CorfuMsgType.SEQUENCER_METRICS_REQUEST.msg());
        SequencerMetrics seqMetrics = cFuture.join();
        assertThat(seqMetrics.getSequencerStatus())
                .isEqualTo(SequencerMetrics.SequencerStatus.READY);
    }

    @Test
    public void tokensAreIncreasing() {
        long lastTokenValue = -1;
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            CompletableFuture<TokenResponse> future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.emptyList())));
            Token thisToken = future.join().getToken();
            assertThat(thisToken.getSequence())
                    .isGreaterThan(lastTokenValue);
            lastTokenValue = thisToken.getSequence();
        }
    }

    @Test
    public void checkTokenPositionWorks() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {

            CompletableFuture<TokenResponse> future1 = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.emptyList())));
            CompletableFuture<TokenResponse> future2 = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.emptyList())));

            Token thisToken = future1.join().getToken();
            Token checkToken = future2.join().getToken();

            assertThat(thisToken)
                    .isEqualTo(checkToken);
        }
    }

    @Test
    public void perStreamCheckTokenPositionWorks() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            CompletableFuture<TokenResponse> future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));

            Token thisTokenA = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamA))));

            long checkTokenA = future.join().getStreamTail(streamA);

            assertThat(thisTokenA.getSequence())
                    .isEqualTo(checkTokenA);

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));

            Token thisTokenB = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamB))));
            long checkTokenB = future.join().getStreamTail(streamB);

            assertThat(thisTokenB.getSequence())
                    .isEqualTo(checkTokenB);

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(0L, Collections.singletonList(streamA))));
            long checkTokenA2 = future.join().getStreamTail(streamA);

            assertThat(checkTokenA2)
                    .isEqualTo(checkTokenA);

            assertThat(thisTokenB.getSequence())
                    .isGreaterThan(checkTokenA);
        }
    }

    @Test
    public void checkBackpointersWork() {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            CompletableFuture<TokenResponse> future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            Token thisTokenA = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            long checkTokenAValue = future.join().getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getSequence())
                    .isEqualTo(checkTokenAValue);

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            Token thisTokenB = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamB))));
            long checkTokenBValue = future.join().getBackpointerMap().get(streamB);

            assertThat(thisTokenB.getSequence())
                    .isEqualTo(checkTokenBValue);

            final long MULTI_TOKEN = 5L;

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            thisTokenA = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            checkTokenAValue = future.join().getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getSequence() + MULTI_TOKEN - 1)
                    .isEqualTo(checkTokenAValue);

            // check the requesting multiple tokens does not break the back-pointer for the multi-entry
            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
            thisTokenA = future.join().getToken();

            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(MULTI_TOKEN, Collections.singletonList(streamA))));
            checkTokenAValue = future.join().getBackpointerMap().get(streamA);

            assertThat(thisTokenA.getSequence()).isEqualTo(checkTokenAValue);

        }
    }

    @Test
    public void SequencerWillResetTails() throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        UUID streamC = UUID.nameUUIDFromBytes("streamC".getBytes());

        CompletableFuture<TokenResponse> future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamA))));
        long tailA = future.join().getToken().getSequence();

        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamB))));
        long tailB = future.join().getToken().getSequence();

        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));
        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(1L, Collections.singletonList(streamC))));

        long tailC = future.join().getToken().getSequence();

        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.emptyList())));
        long globalTail = future.join().getToken().getSequence();

        // Construct new tails
        Map<UUID, StreamAddressSpace> tailMap = new HashMap<>();
        long newTailA = tailA + 2;
        long newTailB = tailB + 1;
        // This one should not be updated
        long newTailC = tailC - 1;

        tailMap.put(streamA, new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(newTailA)));
        tailMap.put(streamB, new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(newTailB)));
        tailMap.put(streamC, new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(newTailC)));

        // Modifying the sequencerEpoch to simulate sequencer reset.
        server.setSequencerEpoch(-1L);
        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.BOOTSTRAP_SEQUENCER,
                new SequencerRecoveryMsg(globalTail + 2, tailMap, 0L, false)));
        future.join();
        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamA))));
        assertThat(future.join().getStreamTail(streamA)).isEqualTo(newTailA);

        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamB))));
        assertThat(future.join().getStreamTail(streamB)).isEqualTo(newTailB);

        // We should have the same value than before
        future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                new TokenRequest(0L, Collections.singletonList(streamC))));
        assertThat(future.join().getStreamTail(streamC)).isEqualTo(newTailC);
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
        CompletableFuture<TokenResponse> future = null;
        for (int i = 0; i < num; i++) {
            future = sendRequest(new CorfuPayloadMsg<>(CorfuMsgType.TOKEN_REQ,
                    new TokenRequest(1L, Collections.singletonList(streamA))));
        }

        future.join();
        assertThat(server.getGlobalLogTail()).isEqualTo(num);

        // Sequencer accepts a delta bootstrap message only if the new epoch is consecutive.
        long newEpoch = server.getServerContext().getServerEpoch() + 1;
        server.getServerContext().setServerEpoch(newEpoch, server.getServerContext().getServerRouter());
        CompletableFuture<Boolean> future1 = sendRequestWithEpoch(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerRecoveryMsg(
                Address.NON_EXIST, Collections.emptyMap(), newEpoch, true)), newEpoch);
        assertThat(future1.join()).isEqualTo(true);
        // Sequencer accepts only a full bootstrap message if the epoch is not consecutive.
        newEpoch = server.getServerContext().getServerEpoch() + 2;
        server.getServerContext().setServerEpoch(newEpoch, server.getServerContext().getServerRouter());
        future1 = sendRequestWithEpoch(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerRecoveryMsg(
                Address.NON_EXIST, Collections.emptyMap(), newEpoch, true)), newEpoch);
        assertThat(future1.join()).isEqualTo(false);
        future1 = sendRequestWithEpoch(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(new SequencerRecoveryMsg(
                num, Collections.singletonMap(streamA, new StreamAddressSpace(Address.NON_ADDRESS,
                Roaring64NavigableMap.bitmapOf(num))), newEpoch, false)), newEpoch);
        assertThat(future1.join()).isEqualTo(true);

        future = sendRequestWithEpoch(CorfuMsgType.TOKEN_REQ.payloadMsg(new TokenRequest(0L, Collections.emptyList())), newEpoch);
        assertThat(future.join())
                .isEqualTo(new TokenResponse(TokenType.NORMAL, TokenResponse.NO_CONFLICT_KEY,
                        TokenResponse.NO_CONFLICT_STREAM, new Token(newEpoch, num - 1),
                        Collections.emptyMap(), Collections.emptyMap()));
    }

}
