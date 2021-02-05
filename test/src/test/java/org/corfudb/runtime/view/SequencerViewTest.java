package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/23/15.
 */
public class SequencerViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canAcquireFirstToken() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void canQueryMultipleStreams() {
        CorfuRuntime r = getDefaultRuntime();

        final int totalStreams = 3;
        UUID stream1 = UUID.randomUUID();
        UUID stream2 = UUID.randomUUID();
        UUID stream3 = UUID.randomUUID();

        assertThat(r.getSequencerView().next(stream1).getToken())
                .isEqualTo(new Token(0l, 0l));
        assertThat(r.getSequencerView().next(stream2).getToken())
                .isEqualTo(new Token( 0l, 1l));

        TokenResponse response = r.getSequencerView().query(stream1, stream2, stream3);
        assertThat(response.getStreamTailsCount()).isEqualTo(totalStreams);
        assertThat(response.getStreamTail(stream1)).isEqualTo(0l);
        assertThat(response.getStreamTail(stream2)).isEqualTo(1l);
        assertThat(response.getStreamTail(stream3)).isEqualTo(Address.NON_EXIST);
    }

    @Test
    public void tokensAreIncrementing() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(0L, 1L));
    }

    @Test
    public void checkTokenWorks() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().query().getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkStreamTokensWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().next(streamA).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().query(streamA))
                .isEqualTo(0L);
        assertThat(r.getSequencerView().next(streamB).getToken())
                .isEqualTo(new Token(0L, 1L));
        assertThat(r.getSequencerView().query(streamB))
                .isEqualTo(1L);
        assertThat(r.getSequencerView().query(streamA))
                .isEqualTo(0L);
    }

    @Test
    public void checkBackPointersWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().next(streamA).getBackpointerMap())
                .containsEntry(streamA, Address.NON_EXIST);
        assertThat(r.getSequencerView().query(new UUID[] {streamA}).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().next(streamB).getBackpointerMap())
                .containsEntry(streamB, Address.NON_EXIST);
        assertThat(r.getSequencerView().query(new UUID[] {streamB}).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().next(streamA).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().next(streamB).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }

    /**
     * Check streamAddressSpace after an epoch is incremented.
     * The call should be retried in case the epoch is changed or cluster connectivity is affected.
     */
    @Test
    public void checkStreamAddressSpaceAcrossEpochs() throws Exception {
        CorfuRuntime controlRuntime = getDefaultRuntime();
        CorfuRuntime r = getNewRuntime(getDefaultNode()).connect();
        Layout originalLayout = controlRuntime.getLayoutView().getLayout();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        // Request 3 tokens on the Sequencer.
        final int tokenCount = 3;
        StreamAddressSpace expectedMap = new StreamAddressSpace();
        for (long i = 0; i < tokenCount; i++) {
            r.getSequencerView().next(streamA);
            expectedMap.addAddress(i);
        }
        // Request StreamAddressSpace should succeed.
        assertThat(r.getSequencerView().getStreamAddressSpace(
                new StreamAddressRange(streamA,  tokenCount, Address.NON_ADDRESS)))
                .isEqualTo(expectedMap);

        // Increment the epoch.
        incrementClusterEpoch(controlRuntime);
        controlRuntime.invalidateLayout();
        Layout newLayout = controlRuntime.getLayoutView().getLayout();
        controlRuntime.getLayoutManagementView().reconfigureSequencerServers(originalLayout, newLayout, false);

        // Request StreamAddressSpace should fail with a WrongEpochException initially
        // This is then retried internally and returned with a valid response.
        assertThat(r.getSequencerView().getStreamAddressSpace(
                new StreamAddressRange(streamA,  tokenCount, Address.NON_ADDRESS)))
                .isEqualTo(expectedMap);
    }
}
