package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;
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
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void tokensAreIncrementing() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
    }

    @Test
    public void checkTokenWorks() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkStreamTokensWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getToken())
                .isEqualTo(new Token(1L, 0L));
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(new Token(0L, 0L));
    }

    @Test
    public void checkBackPointersWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, Address.NON_EXIST);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }
}
