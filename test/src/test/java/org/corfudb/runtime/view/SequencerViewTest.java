package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
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

        UUID stream1 = UUID.randomUUID();
        UUID stream2 = UUID.randomUUID();
        UUID stream3 = UUID.randomUUID();

        assertThat(r.getSequencerView().next(stream1).getToken())
                .isEqualTo(new Token(0l, 0l));
        assertThat(r.getSequencerView().next(stream2).getToken())
                .isEqualTo(new Token(1l, 0l));

        assertThat(r.getSequencerView().query(stream1, stream2, stream3).getStreamTails())
                .containsExactly(0l, 1l, Address.NON_EXIST);
    }

    @Test
    public void tokensAreIncrementing() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().next().getToken())
                .isEqualTo(new Token(1L, 0L));
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
        assertThat(r.getSequencerView().query(streamA).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().next(streamB).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().query(streamB).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().query(streamA).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkBackPointersWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().next(streamA).getBackpointerMap())
                .containsEntry(streamA, Address.NON_EXIST);
        assertThat(r.getSequencerView().query(streamA).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().next(streamB).getBackpointerMap())
                .containsEntry(streamB, Address.NON_EXIST);
        assertThat(r.getSequencerView().query(streamB).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().next(streamA).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().next(streamB).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }
}
