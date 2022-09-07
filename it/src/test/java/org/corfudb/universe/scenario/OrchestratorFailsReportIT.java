package org.corfudb.universe.scenario;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.health.Component;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.health.HealthReport.builder;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;

public class OrchestratorFailsReportIT extends GenericIntegrationTest {


    @Test(timeout = 300000)
    public void orchestratorTaskFailedReport() {

        int firstNodePort = 8080;
        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getServer().healthServerPortBase(new AtomicInteger(firstNodePort));
                fixture.getCluster().numNodes(3);
                fixture.getLogging().enabled(true);
                fixture.getServer().logLevel(Level.DEBUG);
            });

            wf.deploy();
            waitUninterruptibly(Duration.ofSeconds(4));

            UniverseParams params = wf.getFixture().data();

            CorfuCluster<CorfuServer, CorfuClusterParams> corfuCluster = wf.getUniverse()
                    .getGroup(params.getGroupParamByIndex(0).getName());
            List<CorfuServer> allServers = new ArrayList<>(corfuCluster.<CorfuServer>nodes().values().asList());
            final CorfuServer corfuServer = allServers.remove(0);
            System.out.println("Disconnecting: " + corfuServer.getParams().getPort() + " " + corfuServer.getParams().getHealthPort());
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            corfuServer.disconnect();
            waitForLayoutChange(layout -> layout.getUnresponsiveServers().contains(corfuServer.getEndpoint()), corfuClient);
            waitUninterruptibly(Duration.ofSeconds(4));
            final CorfuServer otherServer = allServers.remove(0);

            System.out.println("Other server's report:");
            System.out.println(queryHealthReport(otherServer.getParams().getHealthPort()).asJson());
            HealthReport expectedHealthReport = builder()
                    .status(false)
                    .reason("Some of the services experience runtime health issues")
                    .init(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful")
                    ))
                    .runtime(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "There are nodes in the unresponsive list"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running")
                    ))
                    .build();
            assertThat(queryHealthReport(otherServer.getParams().getHealthPort())).isEqualTo(expectedHealthReport);

            System.out.println("Now trying to perform a clustering operation on a node that cant reach quorum");
            try {
                corfuClient.getManagementView().healNode(corfuServer.getEndpoint(), 3, Duration.ofSeconds(3), Duration.ofMillis(1000));
            } catch (Exception e) {
                System.out.println("Expected that we have error: " + e);
            }



            HealthReport healthReport = queryHealthReport(corfuServer.getParams().getHealthPort());
            expectedHealthReport = builder()
                    .status(false)
                    .reason("Some of the services experience runtime health issues")
                    .init(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful")
                    ))
                    .runtime(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(false, "Failed to execute action HealNodeToLayout for workflow HEAL_NODE"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, "Sequencer requires bootstrap")
                    ))
                    .build();
            System.out.println("Our report:");
            System.out.println(healthReport.asJson());
            System.out.println("Expected report:");
            System.out.println(expectedHealthReport.asJson());
            assertThat(healthReport).isEqualTo(expectedHealthReport);


            System.out.println("Reconnect: " + corfuServer.getParams().getPort() + " " + corfuServer.getParams().getHealthPort());
            corfuServer.reconnect();
            System.out.println("Wait for the healing to take place and the FD report that the node is healed");
            waitForLayoutChange(layout -> layout.getUnresponsiveServers().isEmpty(), corfuClient);
            waitUninterruptibly(Duration.ofSeconds(4));
            healthReport = queryHealthReport(corfuServer.getParams().getHealthPort());
            expectedHealthReport = builder()
                    .status(true)
                    .reason("Healthy")
                    .init(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful")
                    ))
                    .runtime(ImmutableMap.of(
                            Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.LAYOUT_SERVER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.ORCHESTRATOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                            Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running")
                    ))
                    .build();
            System.out.println("Our report again:");
            System.out.println(healthReport.asJson());
            assertThat(healthReport).isEqualTo(expectedHealthReport);
        });
    }
}

