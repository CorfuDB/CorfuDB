package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/1/16.
 */
public class AddressSpaceViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void cacheMissTimesOut() {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        getRuntime().setCacheDisabled(false).connect();

        getRuntime().getAddressSpaceView().setEmptyDuration(Duration.ofNanos(100));
        assertThat(getRuntime().getAddressSpaceView().read(0).getResult().getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.EMPTY);
        getRuntime().getLayoutView().getLayout().getLogUnitClient(0, 0).fillHole(0);
        try {Thread.sleep(100);} catch (InterruptedException e) {// don't do anything
        }
        assertThat(getRuntime().getAddressSpaceView().read(0).getResult().getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.FILLED_HOLE);
    }
}
