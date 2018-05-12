package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.corfudb.infrastructure.management.HealingDetector;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
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

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_1)
                .addUnresponsiveServer(SERVERS.PORT_2)
                .build();
        bootstrapAllServers(layout);

        corfuRuntime = getRuntime(layout).connect();

        healingDetector = new HealingDetector();
        healingDetector.setDetectionPeriodDuration(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        healingDetector.setInterIterationInterval(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
    }

    /**
     * We start off with SERVERS.PORT_1 and SERVERS.PORT_2 as unresponsive.
     * SERVERS.PORT_1 continues to remain unresponsive and SERVERS.PORT_2 has been healed.
     * Polls the 2 failed servers.
     * Returns healed status for SERVERS.PORT_2.
     */
    @Test
    public void pollHealedNodes() {

        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());

        Set<String> expectedResult = new HashSet<>();
        expectedResult.add(getEndpoint(SERVERS.PORT_2));

        assertThat(healingDetector.poll(layout, corfuRuntime).getHealingNodes())
                .isEqualTo(expectedResult);
    }
}
