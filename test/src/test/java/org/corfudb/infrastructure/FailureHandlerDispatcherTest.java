package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the failure handling : Sealing and updating layout
 * depending upon the trigger.
 * Created by zlokhandwala on 11/18/16.
 */
public class FailureHandlerDispatcherTest extends AbstractViewTest {

    /**
     * triggers the handler with failure and checks for update in layout.
     */
    @Test
    public void updateLayoutOnFailure() {

        addServer(9000);
        addServer(9001);
        addServer(9002);

        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addSequencer(9000)
                .addSequencer(9001)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addLogUnit(9002)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(originalLayout);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        corfuRuntime.addLayoutServer(getEndpoint(9000));
        corfuRuntime.addLayoutServer(getEndpoint(9001));
        corfuRuntime.addLayoutServer(getEndpoint(9002));
        corfuRuntime.connect();

        Set<String> failedServers = new HashSet<>();
        failedServers.add(getEndpoint(9002));

        FailureHandlerDispatcher failureHandlerDispatcher = new FailureHandlerDispatcher();
        failureHandlerDispatcher.dispatchHandler(originalLayout, corfuRuntime, failedServers);

        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .addSequencer(9001)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(getLayoutServer(9000).getCurrentLayout())
                .isEqualTo(getLayoutServer(9001).getCurrentLayout())
                .isEqualTo(expectedLayout);
    }
}
