package org.corfudb.infrastructure;

import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.IFailureHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.PurgeFailurePolicy;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(originalLayout);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        corfuRuntime.addLayoutServer(getEndpoint(SERVERS.PORT_0));
        corfuRuntime.addLayoutServer(getEndpoint(SERVERS.PORT_1));
        corfuRuntime.addLayoutServer(getEndpoint(SERVERS.PORT_2));
        corfuRuntime.connect();

        Set<String> failedServers = new HashSet<>();
        failedServers.add(getEndpoint(SERVERS.PORT_2));

        ReconfigurationEventHandler reconfigurationEventHandler = new ReconfigurationEventHandler();
        IFailureHandlerPolicy failureHandlerPolicy = new PurgeFailurePolicy();
        reconfigurationEventHandler.handleFailure(failureHandlerPolicy,
                        originalLayout,
                        corfuRuntime,
                        failedServers,
                        Collections.emptySet());

        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(2L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(getLayoutServer(SERVERS.PORT_0).getCurrentLayout())
                .isEqualTo(getLayoutServer(SERVERS.PORT_1).getCurrentLayout())
                .isEqualTo(expectedLayout);
    }
}
