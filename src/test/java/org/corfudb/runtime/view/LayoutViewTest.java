package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canGetLayout() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        String localAddress = "localhost:9000";
        Layout l = new Layout(
                Collections.singletonList(localAddress),
                Collections.singletonList(localAddress),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList(localAddress)
                                )
                        )
                )),
                1L
        );

        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }
}
