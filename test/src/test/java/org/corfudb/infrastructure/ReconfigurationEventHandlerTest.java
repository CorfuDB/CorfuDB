package org.corfudb.infrastructure;

import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.PurgeFailurePolicy;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the failure handling : Sealing and updating layout
 * depending upon the trigger.
 * Created by zlokhandwala on 11/18/16.
 */
public class ReconfigurationEventHandlerTest extends AbstractViewTest {

    /**
     * triggers the handler with failure and checks for update in layout.
     */
    @Test
    public void updateLayoutOnFailure() {

        addServer(SERVERS.ENDPOINT_0);
        addServer(SERVERS.ENDPOINT_1);
        addServer(SERVERS.ENDPOINT_2);

        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(originalLayout);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        corfuRuntime.addLayoutServer(SERVERS.ENDPOINT_0);
        corfuRuntime.addLayoutServer(SERVERS.ENDPOINT_1);
        corfuRuntime.addLayoutServer(SERVERS.ENDPOINT_2);
        corfuRuntime.connect();

        Set<NodeLocator> failedServers = new HashSet<>();
        failedServers.add(SERVERS.ENDPOINT_2);

        ReconfigurationEventHandler reconfigurationEventHandler = new ReconfigurationEventHandler();
        IReconfigurationHandlerPolicy failureHandlerPolicy = new PurgeFailurePolicy();
        reconfigurationEventHandler.handleFailure(failureHandlerPolicy,
                originalLayout,
                corfuRuntime,
                NodeLocator.transformToStringsSet(failedServers)
        );

        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(getLayoutServer(SERVERS.ENDPOINT_0).getCurrentLayout())
                .isEqualTo(getLayoutServer(SERVERS.ENDPOINT_1).getCurrentLayout())
                .isEqualTo(expectedLayout);
    }
}
