package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
public class ReplexStreamViewTest extends StreamViewTest {

    @Before
    @Override
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
        // First commit a layout that uses Replex
        Layout newLayout = r.layout.get();
        newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.REPLEX);
        newLayout.getSegment(0L).setReplexes(Collections.singletonList(
                new Layout.LayoutStripe(Collections.singletonList(defaultConfigurationString))));
        newLayout.setEpoch(1);
        r.getLayoutView().committed(1L, newLayout);
        r.invalidateLayout();
    }

}
