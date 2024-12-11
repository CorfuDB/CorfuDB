package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.health.HealthReport.ComponentReportedHealthStatus;
import org.corfudb.infrastructure.health.HealthReport.ReportedLivenessStatus;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.Sleep;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.health.Component.COMPACTOR;
import static org.corfudb.infrastructure.health.Component.FAILURE_DETECTOR;
import static org.corfudb.infrastructure.health.Component.LOG_UNIT;
import static org.corfudb.infrastructure.health.Component.ORCHESTRATOR;
import static org.corfudb.infrastructure.health.Component.SEQUENCER;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_INITIALIZED;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_IS_NOT_RUNNING;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_IS_RUNNING;
import static org.corfudb.infrastructure.health.HealthReport.COMPONENT_NOT_INITIALIZED;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.DOWN;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UNKNOWN;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UP;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_DOWN;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_UNKNOWN;
import static org.corfudb.infrastructure.health.HealthReport.OVERALL_STATUS_UP;
import static org.corfudb.infrastructure.health.Issue.IssueId.COMPACTION_CYCLE_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.FAILURE_DETECTOR_TASK_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.SEQUENCER_REQUIRES_FULL_BOOTSTRAP;
import static org.corfudb.infrastructure.health.Issue.IssueId.SOME_NODES_ARE_UNRESPONSIVE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class HealthMonitorTest {

    static final String FD_TASK_FAILURE_MSG = "Last failure detection task failed";
    static final String UNRESPONSIVE_MSG = "Current node is in unresponsive list";

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testAddInitIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).isNotEmpty();
        assertThat(healthStatusSnapshot).containsKey(COMPACTOR);
        assertThat(healthStatusSnapshot.get(COMPACTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(COMPACTOR, Issue.IssueId.INIT, "Compactor is not initialized"));
        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(ORCHESTRATOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(3);
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(FAILURE_DETECTOR, Issue.IssueId.INIT, "Failure Detector is not initialized"));
        assertThat(healthStatusSnapshot.get(ORCHESTRATOR).getInitHealthIssues().stream().findFirst().get())
                .isEqualTo(new Issue(ORCHESTRATOR, Issue.IssueId.INIT, "Clustering Orchestrator is not initialized"));

        for (int i = 0; i < 10; i++) {
            HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        }
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(COMPACTOR).getInitHealthIssues().size()).isEqualTo(1);
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testRemoveInitIssue() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(COMPACTOR).isInitHealthy()).isFalse();
        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(ORCHESTRATOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(COMPACTOR));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(COMPACTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).isInitHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(ORCHESTRATOR).isInitHealthy()).isFalse();
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testAddRuntimeIssue() {
        final String compactionMsg = "Last compaction cycle failed";
        final String fdMsg = FD_TASK_FAILURE_MSG;
        final String unresponsiveMsg = UNRESPONSIVE_MSG;
        final int expectedSize = 2;
        HealthMonitor.init();
        // Init status is UNKNOWN this makes it NOT_INITIALIZED, so you can not add runtime issues
        assertThatThrownBy(() -> HealthMonitor.reportIssue(Issue.createIssue(COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg)))
                .isInstanceOf(IllegalStateException.class);
        // Status is now NOT_INITIALIZED, so runtime issue still can not be added
        HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        assertThatThrownBy(() -> HealthMonitor.reportIssue(Issue.createIssue(COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg)))
                .isInstanceOf(IllegalStateException.class);
        HealthMonitor.resolveIssue(Issue.createInitIssue(COMPACTOR));
        // Status is now INITIALIZED, we can add runtime issues
        HealthMonitor.reportIssue(Issue.createIssue(COMPACTOR, COMPACTION_CYCLE_FAILED, compactionMsg));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(1);
        assertThat(healthStatusSnapshot.get(COMPACTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(COMPACTOR).isRuntimeHealthy()).isFalse();

        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createIssue(FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, fdMsg));
        HealthMonitor.reportIssue(Issue.createIssue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot).hasSize(expectedSize);
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).isInitHealthy()).isTrue();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).isRuntimeHealthy()).isFalse();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).getRuntimeHealthIssues()).hasSize(expectedSize);
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testRemoveRuntimeIssue() {
        String resolved = "Resolved";
        String fdMsg = FD_TASK_FAILURE_MSG;
        String unresponsiveMsg = UNRESPONSIVE_MSG;
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createIssue(FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED, fdMsg));
        HealthMonitor.reportIssue(Issue.createIssue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        Map<Component, HealthStatus> healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).getLatestRuntimeIssue().get())
                .isEqualTo(new Issue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        HealthMonitor.resolveIssue(Issue.createIssue(FAILURE_DETECTOR,
                FAILURE_DETECTOR_TASK_FAILED, resolved));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).getLatestRuntimeIssue().get())
                .isEqualTo(new Issue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, unresponsiveMsg));
        HealthMonitor.resolveIssue(Issue.createIssue(FAILURE_DETECTOR,
                SOME_NODES_ARE_UNRESPONSIVE, resolved));
        healthStatusSnapshot = HealthMonitor.getHealthStatusSnapshot();
        assertThat(healthStatusSnapshot.get(FAILURE_DETECTOR).isRuntimeHealthy()).isTrue();
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testGenerateHealthReport() {
        // If not initialized throws an error
        assertThatThrownBy(HealthMonitor::generateHealthReport).isInstanceOf(IllegalStateException.class);
        HealthMonitor.init();
        HealthReport expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of())
                        .runtime(ImmutableSet.of())
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .status(UNKNOWN)
                        .reason(OVERALL_STATUS_UNKNOWN)
                        .build();

        HealthReport healthReport = HealthMonitor.generateHealthReport();
        // If initialized but init map is empty, the status is unknown
        assertThat(healthReport).isEqualTo(expectedReport);
        // 3 components are not up yet
        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(SEQUENCER));
        HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, DOWN, COMPONENT_NOT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, DOWN, COMPONENT_IS_NOT_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_DOWN)
                        .status(DOWN)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Bring up two components
        HealthMonitor.resolveIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(COMPACTOR));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_DOWN)
                        .status(DOWN)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Bring up all components
        HealthMonitor.resolveIssue(Issue.createInitIssue(SEQUENCER));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_UP)
                        .status(UP)
                        .build();

        assertThat(healthReport).isEqualTo(expectedReport);
        // Another component registers to be initialized
        HealthMonitor.reportIssue(Issue.createInitIssue(LOG_UNIT));
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_NOT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_IS_NOT_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_DOWN)
                        .status(DOWN)
                        .build();

        healthReport = HealthMonitor.generateHealthReport();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Initialize it
        HealthMonitor.resolveIssue(Issue.createInitIssue(LOG_UNIT));
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_UP)
                        .status(UP)
                        .build();
        healthReport = HealthMonitor.generateHealthReport();
        assertThat(healthReport).isEqualTo(expectedReport);
        // FD fails
        String fdFailure = "Failure detector task has failed";
        HealthMonitor.reportIssue(new Issue(FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED,
                fdFailure));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, FAILURE, fdFailure),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Compactor fails
        String compactorFailure = "Compactor failed";
        HealthMonitor.reportIssue(new Issue(COMPACTOR, COMPACTION_CYCLE_FAILED, compactorFailure));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, FAILURE, fdFailure),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, FAILURE, compactorFailure),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Node becomes unresponsive, the most recent failure is reflected in the report
        String nodeUnresponsive = "Node is in the unresponsive list";
        HealthMonitor.reportIssue(new Issue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE, nodeUnresponsive));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, FAILURE, nodeUnresponsive),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, FAILURE, compactorFailure),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Compactor is running again
        HealthMonitor.resolveIssue(new Issue(COMPACTOR, COMPACTION_CYCLE_FAILED, "Compaction finished"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, FAILURE, nodeUnresponsive),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // The unresponsive node is healed -> display the previous issue if it's still there
        HealthMonitor.resolveIssue(new Issue(FAILURE_DETECTOR, SOME_NODES_ARE_UNRESPONSIVE,
                "Node is healed"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, FAILURE, fdFailure),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // The network partition is restored and the node can run FD again
        HealthMonitor.resolveIssue(new Issue(FAILURE_DETECTOR, FAILURE_DETECTOR_TASK_FAILED,
                "Last Failure Detection task ran successfully"));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_UP)
                        .status(UP)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // Node is reset and all the services shut down
        ImmutableList.of(FAILURE_DETECTOR, SEQUENCER, COMPACTOR, LOG_UNIT)
                .forEach(component -> HealthMonitor.reportIssue(Issue.createInitIssue(component)));
        healthReport = HealthMonitor.generateHealthReport();
        expectedReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, DOWN, COMPONENT_NOT_INITIALIZED),
                                new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_NOT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, DOWN, COMPONENT_IS_NOT_RUNNING),
                                new ComponentReportedHealthStatus(LOG_UNIT, DOWN, COMPONENT_IS_NOT_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_DOWN)
                        .status(DOWN)
                        .build();
        assertThat(healthReport).isEqualTo(expectedReport);
        // HealthMonitor is shutdown too
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testDeadlockDetection() {
        HealthMonitor.init();
        HealthMonitor.reportIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.reportIssue(Issue.createInitIssue(SEQUENCER));
        HealthMonitor.reportIssue(Issue.createInitIssue(COMPACTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(FAILURE_DETECTOR));
        HealthMonitor.resolveIssue(Issue.createInitIssue(SEQUENCER));
        HealthMonitor.resolveIssue(Issue.createInitIssue(COMPACTOR));
        HealthReport healthyReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(UP, OVERALL_STATUS_UP))
                        .reason(OVERALL_STATUS_UP)
                        .status(UP)
                        .build();
        assertThat(HealthMonitor.generateHealthReport()).isEqualTo(healthyReport);
        // Deadlock two threads and verify that HealthMonitor is able to detect it
        ReentrantLock lockA = new ReentrantLock();
        ReentrantLock lockB = new ReentrantLock();
        CyclicBarrier cb = new CyclicBarrier(2);

        Runnable block1 = () -> {
            try {
                lockA.lockInterruptibly();
                cb.await();
                lockB.lockInterruptibly();
            }
            catch(InterruptedException ie) {
                // interrupted
            }
            catch (BrokenBarrierException bbe) {
                throw new IllegalStateException();
            }
        };

        // Thread-2
        Runnable block2 = () -> {
            try {
                lockB.lockInterruptibly();
                cb.await();
                lockA.lockInterruptibly();
            }
            catch(InterruptedException ie) {
               // interrupted
            }
            catch (BrokenBarrierException bbe) {
                throw new IllegalStateException();
            }
        };

        final Thread thread1 = new Thread(block1);
        final Thread thread2 = new Thread(block2);
        thread1.start();
        thread2.start();
        while (!lockA.hasQueuedThreads() && !lockB.hasQueuedThreads()) {
            Sleep.sleepUninterruptibly(Duration.ofMillis(50));
        }
        HealthMonitor.liveness();
        final HealthReport healthReport = HealthMonitor.generateHealthReport();
        final String reason = healthReport.getLiveness().getReason();

        HealthReport unhealthyReport =
                HealthReport.builder()
                        .init(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_INITIALIZED),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_INITIALIZED)))
                        .runtime(ImmutableSet.of(
                                new ComponentReportedHealthStatus(FAILURE_DETECTOR, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(SEQUENCER, UP, COMPONENT_IS_RUNNING),
                                new ComponentReportedHealthStatus(COMPACTOR, UP, COMPONENT_IS_RUNNING)))
                        .liveness(new ReportedLivenessStatus(DOWN, reason))
                        .reason(OVERALL_STATUS_FAILURE)
                        .status(FAILURE)
                        .build();
        assertThat(healthReport).isEqualTo(unhealthyReport);
        // Resolve the deadlock and verify that it's reflected in the health report
        thread1.interrupt();
        thread2.interrupt();
        while (lockA.hasQueuedThreads() && lockB.hasQueuedThreads()) {
            Sleep.sleepUninterruptibly(Duration.ofMillis(50));
        }
        HealthMonitor.liveness();
        assertThat(HealthMonitor.generateHealthReport()).isEqualTo(healthyReport);
        HealthMonitor.shutdown();
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    void testRuntimeIssueReportIsResolvedInRescheduledContext() {
        // Schedule a thread that reports the runtime issue
        // but wrap the runnable in runSansThrow, so it's rescheduled
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("sequencer-health")
                        .build());
        AtomicBoolean issuePersists = new AtomicBoolean(false);
        CountDownLatch errorLatch = new CountDownLatch(1);
        CountDownLatch resolutionLatch = new CountDownLatch(1);

        try {
            Runnable run = () -> {
                try {
                    Issue issue = Issue.createIssue(SEQUENCER,
                            SEQUENCER_REQUIRES_FULL_BOOTSTRAP, "Sequencer " +
                                    "requires bootstrap");
                    HealthMonitor.reportIssue(issue);
                    issuePersists.set(false);
                    resolutionLatch.countDown();
                }
                catch (IllegalStateException ie) {
                    issuePersists.set(true);
                    errorLatch.countDown();
                    throw ie;
                }
            };

            HealthMonitor.init();
            HealthMonitor.reportIssue(Issue.createInitIssue(SEQUENCER));
            scheduledExecutorService
                    .scheduleAtFixedRate(
                            () -> LambdaUtils.runSansThrow(run),0,  1, SECONDS);

            if (!errorLatch.await(3, SECONDS)) {
                throw new IllegalStateException("First Condition was not " +
                        "met within the timeout period");
            }

            // Check that the issue persists
            assertTrue(issuePersists.get());
            // Now resolve the init issue, so the runtime issues can be reported
            HealthMonitor.resolveIssue(Issue.createInitIssue(Component.SEQUENCER));

            if (!resolutionLatch.await(3, SECONDS)) {
                throw new IllegalStateException("Second Condition was not " +
                        "met within the timeout period");
            }

            assertFalse(issuePersists.get());
        }

        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test interrupted while waiting for conditions.", e);
        }

        finally {
            HealthMonitor.shutdown();
            scheduledExecutorService.shutdown();
            try {
                if (!scheduledExecutorService.awaitTermination(1, SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
