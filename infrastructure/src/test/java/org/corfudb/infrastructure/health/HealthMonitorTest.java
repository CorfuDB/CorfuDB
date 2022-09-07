package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.health.Issue.IssueId.COMPACTION_CYCLE_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.SOME_NODES_ARE_UNRESPONSIVE;
import static org.corfudb.infrastructure.health.Issue.IssueId.FAILURE_DETECTOR_TASK_FAILED;

public class HealthMonitorTest {

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testAddInitIssue() {
        int nums = 10;
        int expectedIssue = 1;
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).isNotEmpty();
        assertThat(healthStatusSnapshot).containsKey(Component.COMPACTOR);
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.COMPACTOR, Issue.IssueId.INIT, "Compactor is not initialized"));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.ORCHESTRATOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(3);
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.FAILURE_DETECTOR, Issue.IssueId.INIT, "Failure Detector is not initialized"));
        assertThat(healthStatusSnapshot.get(Component.ORCHESTRATOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(Component.ORCHESTRATOR, Issue.IssueId.INIT, "Clustering Orchestrator is not initialized"));

        for (int i = 0; i < nums; i++) {
            HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        }
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).getInitHealthIssues().size()).isEqualTo(expectedIssue);
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testRemoveInitIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isInitHealthy()).isFalse();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.ORCHESTRATOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isInitHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(Component.ORCHESTRATOR).isInitHealthy()).isFalse();
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testAddRuntimeIssue() {
        String compactionMsg = "Last compaction cycle failed";
        String fdMsg = "Last failure detection task failed";
        String unresponsiveMsg = "Current node is in unresponsive list";
        int expectedSize = 2;
        HealthMonitor.init();
        // Init status is UNKNOWN this makes it NOT_INITIALIZED, so you can not add runtime issues
        assertThatThrownBy(() -> HealthMonitor.reportIssue(Issue.createIssue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg)))
                .isInstanceOf(IllegalStateException.class);
        // Status is now NOT_INITIALIZED, so runtime issue still can not be added
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        assertThatThrownBy(() -> HealthMonitor.reportIssue(Issue.createIssue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg)))
                .isInstanceOf(IllegalStateException.class);
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
        // Status is now INITIALIZED, we can add runtime issues
        HealthMonitor.reportIssue(Issue.createIssue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(1);
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(Component.COMPACTOR).isRuntimeHealthy()).isFalse();

        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, fdMsg));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(expectedSize);
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isRuntimeHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getRuntimeHealthIssues()).hasSize(expectedSize);
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testRemoveRuntimeIssue() {
        String resolved = "Resolved";
        String fdMsg = "Last failure detection task failed";
        String unresponsiveMsg = "Current node is in unresponsive list";
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, fdMsg));
        HealthMonitor.reportIssue(Issue.createIssue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getLatestRuntimeIssue().get())
                .isEqualTo(new Issue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        HealthMonitor.resolveIssue(Issue.createIssue(Component.FAILURE_DETECTOR,
                FAILURE_DETECTOR_TASK_FAILED, resolved));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).getLatestRuntimeIssue().get())
                .isEqualTo(new Issue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        HealthMonitor.resolveIssue(Issue.createIssue(Component.FAILURE_DETECTOR,
                SOME_NODES_ARE_UNRESPONSIVE, resolved));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(Component.FAILURE_DETECTOR).isRuntimeHealthy()).isTrue();
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testGenerateHealthReport() {
        // If not initialized throws an error
        String notInitialized = "Service is not initialized";
        String notRunning = "Service is not running";
        assertThatThrownBy(HealthMonitor::generateHealthReport).isInstanceOf(IllegalStateException.class);
        HealthMonitor.init();
        HealthReport expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of())
                        .runtime(ImmutableMap.of())
                        .status(false)
                        .reason("Status is unknown")
                        .build();

        HealthReport healthReport = HealthMonitor.generateHealthReport();
        // If initialized but init map is empty, the status is unknown
        assertThat(healthReport).isEqualTo(expectedReport);
        // 3 components are not up yet
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.SEQUENCER));
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, notInitialized),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, notInitialized),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, notInitialized)))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, notRunning),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, notRunning),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, notRunning)))
                        .reason("Some of the services are not initialized")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Bring up two components
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, "Service is not initialized"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, "Service is not running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services are not initialized")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Bring up all components
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.SEQUENCER));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Healthy")
                        .status(true)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Another component registers to be initialized
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.LOG_UNIT));
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(false, "Service is not initialized")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(false, "Service is not running")))
                        .reason("Some of the services are not initialized")
                        .status(false)
                        .build();
        healthReport = HealthMonitor.generateHealthReport();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Initialize it
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.LOG_UNIT));
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Healthy")
                        .status(true)
                        .build();
        healthReport = HealthMonitor.generateHealthReport();
        assertThat(healthReport).isEqualTo(expectedReport);
        // FD fails
        HealthMonitor.reportIssue(new Issue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Failure detector task has failed"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Failure detector task has failed"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services experience runtime health issues")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Compactor fails
        HealthMonitor.reportIssue(new Issue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, "Compactor failed"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Failure detector task has failed"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, "Compactor failed"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services experience runtime health issues")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Node becomes unresponsive, this is reflected in the report
        HealthMonitor.reportIssue(new Issue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, "Node is in the unresponsive list"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Node is in the unresponsive list"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, "Compactor failed"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services experience runtime health issues")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Compactor is running again
        HealthMonitor.resolveIssue(new Issue(Component.COMPACTOR, COMPACTION_CYCLE_FAILED, "Compaction finished"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Node is in the unresponsive list"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services experience runtime health issues")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // The unresponsive node is healed -> display the previous issue if it's still there
        HealthMonitor.resolveIssue(new Issue(Component.FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, "Node is healed"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Failure detector task has failed"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Some of the services experience runtime health issues")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // The network partition is restored and the node can run FD again
        HealthMonitor.resolveIssue(new Issue(Component.FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, "Last Failure Detection task ran successfully"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Initialization successful"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Initialization successful")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(true, "Up and running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(true, "Up and running")))
                        .reason("Healthy")
                        .status(true)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Node is reset and all the services shut down
        ImmutableList.of(Component.FAILURE_DETECTOR, Component.SEQUENCER, Component.COMPACTOR, Component.LOG_UNIT)
                .forEach(component -> HealthMonitor.reportIssue(Issue.createInitIssue(component)));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Service is not initialized"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, "Service is not initialized"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, "Service is not initialized"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(false, "Service is not initialized")))
                        .runtime(ImmutableMap.of(
                                Component.FAILURE_DETECTOR, new HealthReport.ReportedHealthStatus(false, "Service is not running"),
                                Component.SEQUENCER, new HealthReport.ReportedHealthStatus(false, "Service is not running"),
                                Component.COMPACTOR, new HealthReport.ReportedHealthStatus(false, "Service is not running"),
                                Component.LOG_UNIT, new HealthReport.ReportedHealthStatus(false, "Service is not running")))
                        .reason("Some of the services are not initialized")
                        .status(false)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // HealthMonitor is shutdown too
        HealthMonitor.shutdown();
    }


}
