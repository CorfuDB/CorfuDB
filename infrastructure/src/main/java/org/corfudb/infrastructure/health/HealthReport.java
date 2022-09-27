package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.DOWN;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.FAILURE;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UNKNOWN;
import static org.corfudb.infrastructure.health.HealthReport.ComponentStatus.UP;

/**
 * HealthReport represents the overall health status of the node.
 */
@Builder
@ToString
@EqualsAndHashCode
public class HealthReport {

    /**
     * Overall status msgs
     */
    public static final String OVERALL_STATUS_UNKNOWN = "Status is unknown";
    public static final String OVERALL_STATUS_UP = "Healthy";
    public static final String OVERALL_STATUS_DOWN = "Some of the services are not initialized";
    public static final String OVERALL_STATUS_FAILURE = "Some of the services experience runtime health issues";

    /**
     * Component status msgs
     */
    public static final String COMPONENT_INITIALIZED = "Initialization successful";
    public static final String COMPONENT_NOT_INITIALIZED = "Service is not initialized";
    public static final String COMPONENT_IS_NOT_RUNNING = "Service is not running";
    public static final String COMPONENT_IS_RUNNING = "Up and running";

    /**
     * Health status false = not healthy, true = healthy
     */
    @Builder.Default
    @Getter
    private final ComponentStatus status = UNKNOWN;
    /**
     * Description
     */
    @Builder.Default
    @Getter
    private final String reason = OVERALL_STATUS_UNKNOWN;

    /**
     * Set of component initialization health issues
     */
    @NonNull
    @Getter
    private final Set<ComponentReportedHealthStatus> init;
    /**
     * Set of component runtime health issues
     */
    @NonNull
    @Getter
    private final Set<ComponentReportedHealthStatus> runtime;

    /**
     * Create a HealthReport from the HealthMonitor's componentHealthStatus. Overall status is healthy if all the
     * components are init and runtime healthy. If init report is empty - the overall status is unknown. If at least
     * one init component is unhealthy or at least one runtime component is unhealthy, it's reflected in the overall status.
     * Otherwise, the status is healthy.
     *
     * @param componentHealthStatus HealthMonitor's componentHealthStatus
     * @return A health report
     */
    public static HealthReport fromComponentHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        Map<Component, HealthStatus> componentHealthStatusSnapshot = ImmutableMap.copyOf(componentHealthStatus);
        Set<ComponentReportedHealthStatus> initReportedHealthStatus =
                createInitReportedHealthStatus(componentHealthStatusSnapshot);
        Set<ComponentReportedHealthStatus> runtimeReportedHealthStatus =
                createRuntimeReportedHealthStatus(componentHealthStatusSnapshot);
        String overallReason;
        ComponentStatus overallStatus;
        if (initReportedHealthStatus.isEmpty()) {
            overallStatus = UNKNOWN;
            overallReason = OVERALL_STATUS_UNKNOWN;
        } else if (!isHealthy(initReportedHealthStatus)) {
            overallStatus = DOWN;
            overallReason = OVERALL_STATUS_DOWN;
        } else if (!isHealthy(runtimeReportedHealthStatus)) {
            overallStatus = FAILURE;
            overallReason = OVERALL_STATUS_FAILURE;
        } else {
            overallStatus = UP;
            overallReason = OVERALL_STATUS_UP;
        }
        return HealthReport.builder()
                .status(overallStatus)
                .reason(overallReason)
                .init(initReportedHealthStatus)
                .runtime(runtimeReportedHealthStatus)
                .build();
    }

    /**
     * @return A json representation of a health report
     */
    public String asJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    private static boolean isHealthy(Set<ComponentReportedHealthStatus> componentHealthMap) {
        return !componentHealthMap.isEmpty() && componentHealthMap.stream()
                .allMatch(healthStatus -> healthStatus.status == UP);
    }

    private static Set<ComponentReportedHealthStatus> createInitReportedHealthStatus(Map<Component,
            HealthStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
            if (healthStatus.isInitHealthy()) {
                return new ComponentReportedHealthStatus(component, UP,
                        COMPONENT_INITIALIZED);
            } else {
                return new ComponentReportedHealthStatus(component, DOWN,
                        COMPONENT_NOT_INITIALIZED);
            }
        }).collect(ImmutableSet.toImmutableSet());
    }

    private static Set<ComponentReportedHealthStatus> createRuntimeReportedHealthStatus(Map<Component,
            HealthStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
            final Optional<Issue> maybeLatestRuntimeIssue = healthStatus.getLatestRuntimeIssue();
            if (maybeLatestRuntimeIssue.isPresent()) {
                Issue issue = maybeLatestRuntimeIssue.get();
                return new ComponentReportedHealthStatus(component, FAILURE, issue.getDescription());
            } else if (!healthStatus.isRuntimeHealthy()) {
                return new ComponentReportedHealthStatus(component, DOWN, COMPONENT_IS_NOT_RUNNING);
            } else {
                return new ComponentReportedHealthStatus(component, UP, COMPONENT_IS_RUNNING);
            }
        }).collect(ImmutableSet.toImmutableSet());
    }

    @AllArgsConstructor
    public enum ComponentStatus {

        @SerializedName("UP")
        UP("UP"),

        @SerializedName("DOWN")
        DOWN("DOWN"),

        @SerializedName("FAILURE")
        FAILURE("FAILURE"),

        @SerializedName("UNKNOWN")
        UNKNOWN("UNKNOWN");


        private final String fullName;

        @Override
        public String toString() {
            return fullName;
        }

    }

    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class ComponentReportedHealthStatus {
        @Getter
        private final Component name;

        @Getter
        private final ComponentStatus status;

        @Getter
        private final String reason;
    }
}
