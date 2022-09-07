package org.corfudb.infrastructure.health;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * HealthStatus of a component. It's separated into initHealthIssues and runtimeHealthIssues.
 * initHealthIssues are present only if the component is not initialized, no runtimeHealthIssues
 * are present at that time. Once initHealthIssues is empty (component is initialized),
 * runtimeHealthIssues can appear.
 */
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

    /**
     * Add init health issue -- health monitor now expects the component to be initialized.
     * If component has been not been previously initialized (UNKNOWN) or initialized (INITIALIZED),
     * adding init issue will move it to INITIALIZED, all runtime issues are cleared.
     * @param issue An init issue
     */
    public void addInitHealthIssue(Issue issue) {
        // If component has been not been previously initialized (UNKNOWN) or
        // initialized (INITIALIZED), adding init issue will move it to INITIALIZED,
        // all runtime issues are cleared
        if (initStatus == InitStatus.UNKNOWN || initStatus == InitStatus.INITIALIZED) {
            initStatus = InitStatus.NOT_INITIALIZED;
            runtimeHealthIssues.clear();
        }
        initHealthIssues.add(issue);
    }

    /**
     * Remove the init health issue -- report to health monitor that the component is initialized.
     * @param issue An init issue
     */
    public void resolveInitHealthIssue(Issue issue) {
        if (initStatus == InitStatus.NOT_INITIALIZED) {
            initStatus = InitStatus.INITIALIZED;
        }
        initHealthIssues.remove(issue);
    }

    /**
     * Add runtime health issue -- health monitor now expects the component's runtime issue to recover.
     * If component has been not been previously initialized (INITIALIZED), adding the runtime issue will
     * result in exception.
     * @param issue An init issue
     */
    public void addRuntimeHealthIssue(Issue issue) {
        if (initStatus != InitStatus.INITIALIZED) {
            throw new IllegalStateException("Runtime health issue can only be reported if the component is initialized");
        }
        runtimeHealthIssues.add(issue);
    }

    /**
     * Remove the runtime health issue -- report to health monitor that this issue is resolved.
     * @param issue An init issue
     */
    public void resolveRuntimeHealthIssue(Issue issue) {
        runtimeHealthIssues.remove(issue);
    }

    public boolean isInitHealthy() {
        return initHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    public boolean isRuntimeHealthy() {
        return runtimeHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    /**
     * Get the last reported runtime issue
     * @return A possible runtime health issue
     */
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
