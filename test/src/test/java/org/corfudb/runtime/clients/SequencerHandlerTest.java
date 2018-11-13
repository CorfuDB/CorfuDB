package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;

import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.protocols.wireprotocol.LogicalSequenceNumber;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
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
        LogicalSequenceNumber logicalSequenceNumber = client.nextToken(Collections.emptyList(), 1).get().getLogicalSequenceNumber();
        LogicalSequenceNumber logicalSequenceNumber2 = client.nextToken(Collections.emptyList(), 1).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumber2.getSequenceNumber())
                .isGreaterThan(logicalSequenceNumber.getSequenceNumber());
    }

    @Test
    public void checkTokenPositionWorks()
            throws Exception {
        LogicalSequenceNumber logicalSequenceNumber = client.nextToken(Collections.emptyList(), 1).get().getLogicalSequenceNumber();
        LogicalSequenceNumber logicalSequenceNumber2 = client.nextToken(Collections.emptyList(), 0).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumber)
                .isEqualTo(logicalSequenceNumber2);
    }

    @Test
    public void perStreamTokensWork()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        client.nextToken(Collections.singletonList(streamA), 1).get();
        LogicalSequenceNumber logicalSequenceNumberA = client.nextToken(Collections.singletonList(streamA), 1).get().getLogicalSequenceNumber();
        LogicalSequenceNumber logicalSequenceNumberA2 = client.nextToken(Collections.singletonList(streamA), 0).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumberA)
                .isEqualTo(logicalSequenceNumberA2);
        LogicalSequenceNumber logicalSequenceNumberB = client.nextToken(Collections.singletonList(streamB), 0).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumberB)
                .isNotEqualTo(logicalSequenceNumberA2);
        LogicalSequenceNumber logicalSequenceNumberB2 = client.nextToken(Collections.singletonList(streamB), 1).get().getLogicalSequenceNumber();
        LogicalSequenceNumber logicalSequenceNumberB3 = client.nextToken(Collections.singletonList(streamB), 0).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumberB2)
                .isEqualTo(logicalSequenceNumberB3);
        LogicalSequenceNumber logicalSequenceNumberA3 = client.nextToken(Collections.singletonList(streamA), 0).get().getLogicalSequenceNumber();
        assertThat(logicalSequenceNumberA3)
                .isEqualTo(logicalSequenceNumberA2);
    }
}
