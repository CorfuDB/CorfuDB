package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/18/16.
 */
public class StreamsViewTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canCopyStream()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        UUID streamACopy = CorfuRuntime.getStreamID("stream A copy");
        byte[] testPayload = "hello world".getBytes();
        byte[] testPayloadCopy = "hello world copy".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getResult().getPayload(r))
                .isEqualTo(testPayload);
        assertThat(sv.read())
                .isEqualTo(null);

        StreamView svCopy = r.getStreamsView().copy(streamA, streamACopy, sv.getLogPointer()-1L);

        assertThat(svCopy.read().getResult().getPayload(r))
                .isEqualTo(testPayload);
        assertThat(svCopy.read())
                .isEqualTo(null);

        svCopy.write(testPayloadCopy);

        assertThat(svCopy.read().getResult().getPayload(r))
                .isEqualTo(testPayloadCopy);
        assertThat(svCopy.read())
                .isEqualTo(null);
        assertThat(sv.read())
                .isEqualTo(null);
    }

}
