package org.corfudb.runtime.view.stream;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the BackpointerStreamView
 * <p>
 * Created by zlokhandwala on 5/24/17.
 */
public class BackpointerStreamViewTest extends AbstractViewTest {

    /**
     * Tests the hasNext functionality of the streamView.
     */
    @Test
    public void testGetHasNext() {
        addServer(SERVERS.PORT_0);
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        bootstrapAllServers(layout);

        CorfuRuntime runtime = getRuntime(layout).connect();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        sv.append("hello world".getBytes());

        assertThat(sv.hasNext()).isTrue();
        sv.next();
        assertThat(sv.hasNext()).isFalse();
    }
}
