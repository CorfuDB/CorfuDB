package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.corfudb.infrastructure.management.HealingDetector;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the HealingDetector.
 * <p>
 * Created by zlokhandwala on 10/26/16.
 */
public class HealingDetectorTest extends AbstractViewTest {

    private Layout layout = null;
    private CorfuRuntime corfuRuntime = null;
    private HealingDetector healingDetector = null;

    @Before
    public void pollingEnvironmentSetup() {

        addServer(SERVERS.ENDPOINT_0);
        addServer(SERVERS.ENDPOINT_1);
        addServer(SERVERS.ENDPOINT_2);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.ENDPOINT_1)
                .addUnresponsiveServer(SERVERS.ENDPOINT_2)
                .build();
        bootstrapAllServers(layout);

        corfuRuntime = getRuntime(layout).connect();

        healingDetector = new HealingDetector();
        healingDetector.setDetectionPeriodDuration(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        healingDetector.setInterIterationInterval(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
    }

    /**
     * We start off with SERVERS.ENDPOINT_1 and SERVERS.ENDPOINT_2 as unresponsive.
     * SERVERS.ENDPOINT_1 continues to remain unresponsive and SERVERS.ENDPOINT_2 has been healed.
     * Polls the 2 failed servers.
     * Returns healed status for SERVERS.ENDPOINT_2.
     */
    @Test
    public void pollHealedNodes() {

        addServerRule(SERVERS.ENDPOINT_1, new TestRule().always().drop());

        Set<NodeLocator> expectedResult = new HashSet<>();
        expectedResult.add(SERVERS.ENDPOINT_2);

        Set<NodeLocator> result = healingDetector.poll(layout, corfuRuntime).getHealingNodesEndpoints();
        assertThat(result).isEqualTo(expectedResult);
    }
}
