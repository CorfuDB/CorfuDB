package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class HealthMonitor {

    private final ConcurrentMap<Component, HealthStatus> componentHealthStatus;

    private static Optional<HealthMonitor> instance = Optional.empty();

    private HealthMonitor() {
        this.componentHealthStatus = new ConcurrentHashMap<>();
    }

    public static void init() {
        instance = Optional.of(new HealthMonitor());
    }

    private static final String ERR_MSG = "Health Monitor is not initialized";

    public static void reportIssue(Issue issue) {
        instance.ifPresent(monitor -> monitor.addIssue(issue));
    }

    private void addIssue(Issue issue) {
        componentHealthStatus.compute(issue.getComponent(), (i, hs) -> {
            HealthStatus healthStatus;
            if (hs == null) {
                healthStatus = new HealthStatus();
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
    }

    public static void shutdown() {
        instance.ifPresent(HealthMonitor::close);
    }

    private HealthReport healthReport() {
        return HealthReport.fromComponentHealthStatus(componentHealthStatus);
    }

    public static HealthReport generateHealthReport() {
        return instance.map(HealthMonitor::healthReport).orElseThrow(() -> new IllegalStateException(ERR_MSG));
    }

    private boolean containsIssue(Issue issue) {
        if (componentHealthStatus.containsKey(issue.getComponent())) {
            final HealthStatus healthStatus = componentHealthStatus.get(issue.getComponent());
            return healthStatus.containsIssue(issue);
        }
        return false;
    }

    public static boolean hasIssue(Issue issue) {
        return instance.map(monitor -> monitor.containsIssue(issue)).orElse(false);
    }

    public static Map<Component, HealthStatus> getHealthStatusSnapshot() {
        return instance.map(monitor -> ImmutableMap.copyOf(monitor.componentHealthStatus))
                .orElseThrow(() -> new IllegalStateException(ERR_MSG));
    }

    public static boolean isInit() {
        return instance.isPresent();
    }

}
