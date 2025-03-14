package org.corfudb.universe.scenario;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.health.HealthReport;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.health.Component.FAILURE_DETECTOR;
import static org.corfudb.infrastructure.health.Component.LAYOUT_SERVER;
import static org.corfudb.infrastructure.health.Component.LOG_UNIT;
import static org.corfudb.infrastructure.health.Component.ORCHESTRATOR;
import static org.corfudb.infrastructure.health.Component.SEQUENCER;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_INITIALIZED;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_IS_RUNNING;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UP;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_UP;
import static org.corfudb.infrastructure.health.HealthReport.builder;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForHealthReport;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;

public class ClusteringFailuresHealthReportIT extends GenericIntegrationTest {

    private HealthReport getHealTaskFailedHealthReport() {
        return builder()
                .status(FAILURE)
                .reason(OVERALL_STATUS_FAILURE)
                .init(ImmutableSet.of(
                        new HealthReport.ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new HealthReport.ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(ORCHESTRATOR, FAILURE,
                                "Failed to execute action HealNodeToLayout for workflow HEAL_NODE"),
                        new HealthReport.ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(SEQUENCER, FAILURE,
                                "Sequencer requires bootstrap")))
                .liveness(new HealthReport.ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                .build();
    }

    private HealthReport getHealthyReport() {
        return builder()
                .status(UP)
                .reason(OVERALL_STATUS_UP)
                .init(ImmutableSet.of(
                        new HealthReport.ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                        new HealthReport.ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED)))
                .runtime(ImmutableSet.of(
                        new HealthReport.ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(LAYOUT_SERVER, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(ORCHESTRATOR, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                        new HealthReport.ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING)))
                .liveness(new HealthReport.ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                .build();
    }

    @Test(timeout = 300000)
    @SuppressWarnings("checkstyle:magicnumber")
    public void clusteringFailuresHealthReport() {
        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getCluster().numNodes(3);
                fixture.getLogging().enabled(true);
                fixture.getServer().logLevel(Level.DEBUG);
            });

            wf.deploy();
            waitUninterruptibly(Duration.ofSeconds(4));

            UniverseParams params = wf.getFixture().data();

            CorfuCluster<CorfuServer, CorfuClusterParams> corfuCluster = wf.getUniverse()
                    .getGroup(params.getGroupParamByIndex(0).getName());
            List<CorfuServer> allServers = new ArrayList<>(corfuCluster.nodes().values().asList());
            final CorfuServer corfuServer = allServers.remove(0);
            // Disconnecting the first server
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            corfuServer.disconnect();
            // Wait for layout change and find the unresponsive nodes in other node's health report
            waitForLayoutChange(layout -> layout.getUnresponsiveServers().contains(corfuServer.getEndpoint()), corfuClient);

            // Now trying to perform a clustering operation on a node that cant reach quorum
            try {
                corfuClient.getManagementView().healNode(corfuServer.getEndpoint(), 3, Duration.ofSeconds(3), Duration.ofMillis(1000));
            } catch (Exception e) {
                // Expected that we have error
            }
            waitForHealthReport(
                    report -> report.equals(getHealTaskFailedHealthReport()),
                    () -> queryHealthReport(corfuServer.getParams().getHealthPort())
            );
            // Now reconnect the node and wait for the healing to take place and the FD report that the node is healed
            corfuServer.reconnect();
            waitForLayoutChange(layout -> layout.getUnresponsiveServers().isEmpty(), corfuClient);
            waitForHealthReport(
                    report -> report.equals(getHealthyReport()),
                    () -> queryHealthReport(corfuServer.getParams().getHealthPort())
            );
        });
    }
}

