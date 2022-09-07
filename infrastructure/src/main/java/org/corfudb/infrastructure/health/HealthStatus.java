package org.corfudb.infrastructure.health;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

@ToString
public class HealthStatus {

    @Getter
    private final Set<Issue> initHealthIssues;
    @Getter
    private final Set<Issue> runtimeHealthIssues;

    private InitStatus initStatus = InitStatus.UNKNOWN;

    public HealthStatus() {
        this.initHealthIssues = new LinkedHashSet<>();
        this.runtimeHealthIssues = new LinkedHashSet<>();
    }

    public void addInitHealthIssue(Issue issue) {
        if (initStatus == InitStatus.UNKNOWN || initStatus == InitStatus.INITIALIZED) {
            initStatus = InitStatus.NOT_INITIALIZED;
            runtimeHealthIssues.clear();
        }
        initHealthIssues.add(issue);
    }

    public void resolveInitHealthIssue(Issue issue) {
        if (initStatus == InitStatus.NOT_INITIALIZED) {
            initStatus = InitStatus.INITIALIZED;
        }
        initHealthIssues.remove(issue);
    }

    public void addRuntimeHealthIssue(Issue issue) {
        if (initStatus != InitStatus.INITIALIZED) {
            throw new IllegalStateException("Runtime health issue can only be reported if the component is initialized");
        }
        runtimeHealthIssues.add(issue);
    }

    public void resolveRuntimeHealthIssue(Issue issue) {
        runtimeHealthIssues.remove(issue);
    }

    public boolean isInitHealthy() {
        return initHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    public boolean isRuntimeHealthy() {
        return runtimeHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    public Optional<Issue> getLatestRuntimeIssue() {
        if (runtimeHealthIssues.isEmpty()) {
            return Optional.empty();
        }
        return runtimeHealthIssues.stream().skip(runtimeHealthIssues.size() - 1).findFirst();
    }

    public boolean containsIssue(Issue issue) {
        return initHealthIssues.contains(issue) || runtimeHealthIssues.contains(issue);
    }

    enum InitStatus {
        UNKNOWN,
        NOT_INITIALIZED,
        INITIALIZED
    }
}
