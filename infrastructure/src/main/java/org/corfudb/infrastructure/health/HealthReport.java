package org.corfudb.infrastructure.health;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.corfudb.common.util.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Builder
@ToString
@EqualsAndHashCode
public class HealthReport {

    @Builder.Default
    @Getter
    private final boolean status = false;
    @Builder.Default
    @Getter
    private final String reason = "Unknown";
    @NonNull
    @Getter
    private final Map<Component, ReportedHealthStatus> init;
    @NonNull
    @Getter
    private final Map<Component, ReportedHealthStatus> runtime;


    public static HealthReport fromComponentHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        Map<Component, HealthStatus> componentHealthStatusSnapshot = ImmutableMap.copyOf(componentHealthStatus);
        final Map<Component, ReportedHealthStatus> initReportedHealthStatus = createInitReportedHealthStatus(componentHealthStatusSnapshot);
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
                .runtime(runtimeReportedHealthStatus)
                .build();
    }

    public String asJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(this);
    }

    private static boolean isHealthy(Map<Component, ReportedHealthStatus> componentHealthMap) {
        return !componentHealthMap.isEmpty() && componentHealthMap.values().stream()
                .allMatch(healthStatus -> healthStatus.status);
    }

    private static Map<Component, ReportedHealthStatus> createInitReportedHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
            if (healthStatus.isInitHealthy()) {
                return Tuple.of(component, new ReportedHealthStatus(true, "Initialization successful"));
            } else {
                return Tuple.of(component, new ReportedHealthStatus(false, "Service is not initialized"));
            }
        }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
    }

    private static Map<Component, ReportedHealthStatus> createRuntimeReportedHealthStatus(Map<Component, HealthStatus> componentHealthStatus) {
        return componentHealthStatus.entrySet().stream().map(entry -> {
            final Component component = entry.getKey();
            final HealthStatus healthStatus = entry.getValue();
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
}
