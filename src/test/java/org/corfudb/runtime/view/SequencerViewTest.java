package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.SequencerServer;
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
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1))
                .isEqualTo(0);
    }

    @Test
    public void tokensAreIncrementing() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1))
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1))
                .isEqualTo(1);
    }

    @Test
    public void checkTokenWorks() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1))
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 0))
                .isEqualTo(0);
    }

    @Test
    public void checkStreamTokensWork() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1))
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0))
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1))
                .isEqualTo(1);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0))
                .isEqualTo(1);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0))
                .isEqualTo(0);
    }
}
