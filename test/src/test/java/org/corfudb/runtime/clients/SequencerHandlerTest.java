package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;

import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.Token;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class SequencerHandlerTest extends AbstractClientTest {

    private SequencerClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new SequencerServer(defaultServerContext()))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        SequencerHandler sequencerHandler = new SequencerHandler();
        client = new SequencerClient(router, 0L);
        return new ImmutableSet.Builder<IClient>()
                .add(sequencerHandler)
                .build();
    }

    @Before
    public void bootstrapSequencer() {
        client.bootstrap(0L, Collections.emptyMap(), 0L, false);
    }

    @Test
    public void requestSequencerMetrics()
            throws Exception {
        SequencerMetrics sequencerMetrics = client.requestMetrics().get();
        assertThat(sequencerMetrics.getSequencerStatus()).isEqualTo(SequencerStatus.READY);
    }

    @Test
    public void canGetAToken()
            throws Exception {
        client.nextToken(Collections.emptyList(), 1).get();
    }

    @Test
    public void tokensAreIncrementing()
            throws Exception {
        Token token = client.nextToken(Collections.emptyList(), 1).get().getToken();
        Token token2 = client.nextToken(Collections.emptyList(), 1).get().getToken();
        assertThat(token2.getTokenValue())
                .isGreaterThan(token.getTokenValue());
    }

    @Test
    public void checkTokenPositionWorks()
            throws Exception {
        Token token = client.nextToken(Collections.emptyList(), 1).get().getToken();
        Token token2 = client.nextToken(Collections.emptyList(), 0).get().getToken();
        assertThat(token)
                .isEqualTo(token2);
    }

    @Test
    public void perStreamTokensWork()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        client.nextToken(Collections.singletonList(streamA), 1).get();
        Token tokenA = client.nextToken(Collections.singletonList(streamA), 1).get().getToken();
        Token tokenA2 = client.nextToken(Collections.singletonList(streamA), 0).get().getToken();
        assertThat(tokenA)
                .isEqualTo(tokenA2);
        Token tokenB = client.nextToken(Collections.singletonList(streamB), 0).get().getToken();
        assertThat(tokenB)
                .isNotEqualTo(tokenA2);
        Token tokenB2 = client.nextToken(Collections.singletonList(streamB), 1).get().getToken();
        Token tokenB3 = client.nextToken(Collections.singletonList(streamB), 0).get().getToken();
        assertThat(tokenB2)
                .isEqualTo(tokenB3);
        Token tokenA3 = client.nextToken(Collections.singletonList(streamA), 0).get().getToken();
        assertThat(tokenA3)
                .isEqualTo(tokenA2);
    }
}
