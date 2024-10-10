package org.corfudb.infrastructure.health;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.health.HealthReport.ComponentStatus;
import org.corfudb.util.LambdaUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.*;

/**
 * HealthMonitor keeps track of the HealthStatus of each Component.
 */
@Slf4j
public final class HealthMonitor {

    private static final String ERR_MSG = "Health Monitor is not initialized";

    private final ConcurrentMap<Component, HealthStatus> componentHealthStatus;

    private final ScheduledExecutorService scheduledExecutorService;

    private final AtomicReference<LivenessStatus> livenessStatus;

    private static final Duration LIVENESS_INTERVAL = Duration.ofMinutes(1);

    private static final Map<ComponentStatus, Integer> statusMap = Map.of(
            UNKNOWN, -1, UP, 0, FAILURE, 1, DOWN, 2);

    private final Optional<AtomicInteger> overallHealthStatus;

    private static Optional<HealthMonitor> instance = Optional.empty();

    private HealthMonitor() {
        this.componentHealthStatus = new ConcurrentHashMap<>();
        this.livenessStatus = new AtomicReference<>(new LivenessStatus(true, ""));
        this.overallHealthStatus =
                MicroMeterUtils.gauge("overall.status", new AtomicInteger(-1));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("HealthMonitorLivenessThread")
                        .build());
        this.scheduledExecutorService.scheduleWithFixedDelay(
                () -> LambdaUtils.runSansThrow(() -> {
                    checkLiveness();
                    updateOverallStatus();
                }),
                0,
                LIVENESS_INTERVAL.toMillis(),
                TimeUnit.MILLISECONDS
        );
        log.debug("HealthMonitor is ready.");

    }

    public static void init() {
        instance = Optional.of(new HealthMonitor());
    }

    /**
     * Report the issue that will be reflected in the health report.
     * @param issue An issue
     */
    public static void reportIssue(Issue issue) {
        instance.ifPresent(monitor -> monitor.addIssue(issue));
    }

    private void addIssue(Issue issue) {
        componentHealthStatus.compute(issue.getComponent(), (component, hs) -> {
            HealthStatus healthStatus;
            if (hs == null) {
                healthStatus = new HealthStatus(component);
            } else {
                healthStatus = hs;
            }
            if (issue.isInitIssue()) {
                healthStatus.addInitHealthIssue(issue);
            } else {
                healthStatus.addRuntimeHealthIssue(issue);
            }
            return healthStatus;

        });
    }

    @VisibleForTesting
    static void liveness() {
        instance.ifPresent(HealthMonitor::checkLiveness);
    }

    private void updateOverallStatus() {
        final HealthReport healthReport = generateHealthReport();
        log.debug("Health Report: {}", healthReport.asJson());
        final ComponentStatus status = healthReport.getStatus();
        final int metricStatus = statusMap.get(status);
        overallHealthStatus.ifPresent(ohs -> ohs.set(metricStatus));
    }

    private void checkLiveness() {
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        long[] ids = tmx.findDeadlockedThreads();
        if (ids != null) {
            log.warn("Detected deadlock");
            ThreadInfo[] infos = tmx.getThreadInfo(ids, true, true);
            StringBuilder sb = new StringBuilder();
            for (ThreadInfo ti : infos) {
                sb.append(ti.toString());
                sb.append("\n");
            }
            livenessStatus.set(new LivenessStatus(false, sb.toString()));
        }
        else {
            livenessStatus.set(new LivenessStatus(true, ""));
        }
    }

    /**
     * Resolve the issue. If there was no issue to begin with, it's a NOOP.
     * @param issue An issue
     */
    public static void resolveIssue(Issue issue) {
        instance.ifPresent(monitor -> monitor.removeIssue(issue));
    }

    private void removeIssue(Issue issue) {
        componentHealthStatus.computeIfPresent(issue.getComponent(), (i, hs) -> {
            if (issue.isInitIssue()) {
                hs.resolveInitHealthIssue(issue);
            } else {
                hs.resolveRuntimeHealthIssue(issue);
            }
            return hs;
        });
    }

    private void close() {
        componentHealthStatus.clear();
        instance = Optional.empty();
        scheduledExecutorService.shutdown();
    }

    public static void shutdown() {
        instance.ifPresent(HealthMonitor::close);
    }

    private HealthReport healthReport() {
        return HealthReport.fromComponentHealthStatus(componentHealthStatus, livenessStatus.get());
    }

    /**
     * Generate health report from the current componentHealthStatus map.
     * @return A health report
     */
    public static HealthReport generateHealthReport() {
        return instance.map(HealthMonitor::healthReport).orElseThrow(() -> new IllegalStateException(ERR_MSG));
    }

    @VisibleForTesting
    public static Map<Component, HealthStatus> getHealthStatusSnapshot() {
        return instance.map(monitor -> ImmutableMap.copyOf(monitor.componentHealthStatus))
                .orElseThrow(() -> new IllegalStateException(ERR_MSG));
    }

    public static boolean isInit() {
        return instance.isPresent();
    }

}
