package org.corfudb.runtime.view;

import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Maithem on 12/18/19.
 */
public class StreamsViewTest extends AbstractViewTest {

    @Before
    public void setRuntime() {
        getDefaultRuntime().connect();
    }

    @Test
    public void testStreamsViewClear() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        IStreamView sv1 = getRuntime().getStreamsView().get(id1);
        IStreamView sv2 = getRuntime().getStreamsView().get(id1);
        IStreamView sv3 = getRuntime().getStreamsView().get(id2);
        IStreamView sv4 = getRuntime().getStreamsView().getUnsafe(id2);
        assertThat(getRuntime().getStreamsView().getOpenedStreams()).containsExactly(sv1, sv2, sv3);
        getRuntime().getStreamsView().clear();
        assertThat(getRuntime().getStreamsView().getOpenedStreams()).isEmpty();
    }
}
