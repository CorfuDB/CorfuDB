package org.corfudb.replayer;

import org.corfudb.generator.replayer.Replayer;
import org.corfudb.generator.replayer.ReplayerUtil;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Sam Behnam on 4/16/18.
 */
public class ReplayWorkloadTest extends AbstractViewTest {
    public static final double OFFSET_MARGIN_PERCENTAGE = 0.2;
    public static final int EXPECTED_EXECUTION_CORFUMAP_TEST = 30;
    public static final int EXPECTED_EXECUTION_NSG_CREATION1M_TEST = 180;
    public static final int EXPECTED_EXECUTION_NSG_LONG_TEST = 540;
    private CorfuRuntime runtime;

    @Before
    public void setRuntime() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);
        runtime = getRuntime().connect();
    }

    @Test
    public void replayCorfuMapTest() throws Exception {
        final List<String> pathsToEventQueues =
                Arrays.asList("src/test/resources/replayerWorkloads/corfuMapTestWorkload.list");

        final long executionTime = Replayer.replayEventList(pathsToEventQueues, runtime);
        final long executionTimeInSeconds = TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS);
        final long threshold = (long)(EXPECTED_EXECUTION_CORFUMAP_TEST * (1 + OFFSET_MARGIN_PERCENTAGE));
        testStatus += String.format("ReplayTime=%ds; Threshold=%ds", executionTimeInSeconds, threshold);
        assertThat(executionTimeInSeconds).isLessThanOrEqualTo(threshold);
    }

    @Test
    public void replayCreateNsgTest() throws Exception {
        final List<String> pathsToEventQueues =
                Arrays.asList("src/test/resources/replayerWorkloads/dfw-600k-operations.list");

        final long executionTime = Replayer.replayEventList(pathsToEventQueues, runtime);
        final long executionTimeInSeconds = TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS);
        final long threshold = (long)(EXPECTED_EXECUTION_NSG_CREATION1M_TEST * (1 + OFFSET_MARGIN_PERCENTAGE));
        testStatus += String.format("ReplayTime=%ds; Threshold=%ds", executionTimeInSeconds, threshold);
        assertThat(executionTimeInSeconds).isLessThanOrEqualTo(threshold);
    }

    @Test
    public void replayCreateNsgLongTest() throws Exception {
        final String persistedListOfEventListFiles = "src/test/resources/replayerWorkloads/nsg-experiments-long.txt";
        final List<String> pathsToEventQueues = ReplayerUtil.retrieveListOfPathToEventQueues(persistedListOfEventListFiles);

        final long executionTime = Replayer.replayEventList(pathsToEventQueues, runtime);
        final long executionTimeInSeconds = TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS);
        final long threshold = (long)(EXPECTED_EXECUTION_NSG_LONG_TEST * (1 + OFFSET_MARGIN_PERCENTAGE));
        testStatus += String.format("ReplayTime=%ds; Threshold=%ds", executionTimeInSeconds, threshold);
        assertThat(executionTimeInSeconds).isLessThanOrEqualTo(threshold);
    }

}
