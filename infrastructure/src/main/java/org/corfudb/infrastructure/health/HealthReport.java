package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.common.util.Tuple;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HealthReport represents the overall health status of the node.
 */
@Builder
@ToString
@EqualsAndHashCode
public class HealthReport {
    /**
     * Health status false = not healthy, true = healthy
     */
    @Builder.Default
    @Getter
    private final boolean status = false;
    /**
     * Description
     */
    @Builder.Default
    @Getter
    private final String reason = "Unknown";

    @NonNull
    @Getter
    private final List<Map.Entry<Component, ReportedHealthStatus>> initList;

    /**
     * Map of component initialization health issues
     */
    @NonNull
    @Getter
    private final Map<Component, ReportedHealthStatus> init;
    /**
     * Map of component runtime health issues
     */
    @NonNull
    @Getter
    private final Map<Component, ReportedHealthStatus> runtime;

    /**
     * Create a HealthReport from the HealthMonitor's componentHealthStatus. Overall status is healthy if all the
     * components are init and runtime healthy. If init report is empty - the overall status is unknown. If at least
     * one init component is unhealthy or at least one runtime component is unhealthy, it's reflected in the overall status.
     * Otherwise, the status is healthy.
     *
     * @param componentHealthStatus HealthMonitor's componentHealthStatus
     * @return A health report
     */
    public static HealthReport fromComponentHealthStatus(Map<Component, ComponentStatus> componentHealthStatus) {
        Map<Component, ComponentStatus> componentHealthStatusSnapshot = ImmutableMap.copyOf(componentHealthStatus);
        final Map<Component, ReportedHealthStatus> initReportedHealthStatus = createInitReportedHealthStatus(componentHealthStatusSnapshot);
        final List<Map.Entry<Component, ReportedHealthStatus>> collect = initReportedHealthStatus.entrySet().stream().collect(ImmutableList.toImmutableList());
        final Map<Component, ReportedHealthStatus> runtimeReportedHealthStatus = createRuntimeReportedHealthStatus(componentHealthStatusSnapshot);
        boolean overallStatus = isHealthy(initReportedHealthStatus) && isHealthy(runtimeReportedHealthStatus);
        String overallReason;
        if (initReportedHealthStatus.isEmpty()) {
            overallReason = "Status is unknown";
        } else if (!isHealthy(initReportedHealthStatus)) {
            overallReason = "Some of the services are not initialized";
        } else if (!isHealthy(runtimeReportedHealthStatus)) {
            overallReason = "Some of the services experience runtime health issues";
        } else {
            overallReason = "Healthy";
        }
        return HealthReport.builder()
                .status(overallStatus)
                .reason(overallReason)
                .init(initReportedHealthStatus)
                .initList(collect)
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

    private static boolean isHealthy(Map<Component, ReportedHealthStatus> componentHealthMap) {
        return !componentHealthMap.isEmpty() && componentHealthMap.values().stream()
                .allMatch(healthStatus -> healthStatus.status);
    }

    private static Map<Component, ReportedHealthStatus> createInitReportedHealthStatus(Map<Component, ComponentStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final ComponentStatus healthStatus = entry.getValue();
            if (healthStatus.isInitHealthy()) {
                return Tuple.of(component, new ReportedHealthStatus(true, "Initialization successful"));
            } else {
                return Tuple.of(component, new ReportedHealthStatus(false, "Service is not initialized"));
            }
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
    }

    private static Map<Component, ReportedHealthStatus> createRuntimeReportedHealthStatus(Map<Component, ComponentStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final ComponentStatus healthStatus = entry.getValue();
            if (healthStatus.getLatestRuntimeIssue().isPresent()) {
                Issue issue = healthStatus.getLatestRuntimeIssue().get();
                return Tuple.of(component, new ReportedHealthStatus(false, issue.getDescription()));
            } else if (!healthStatus.isRuntimeHealthy()) {
                return Tuple.of(component, new ReportedHealthStatus(false, "Service is not running"));
            } else {
                return Tuple.of(component, new ReportedHealthStatus(true, "Up and running"));
            }
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
    }

    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class ReportedHealthStatus {
        @Getter
        private final boolean status;
        @Getter
        private final String reason;
    }

    @AllArgsConstructor
    public enum ComponentStatus {

        @SerializedName("UP")
        UP("UP"),

        @SerializedName("DOWN")
        DOWN("DOWN"),

        @SerializedName("DEGRADED")
        DEGRADED("DEGRADED");


        private final String fullName;

        @Override
        public String toString() {
            return fullName;
        }

        }
}
