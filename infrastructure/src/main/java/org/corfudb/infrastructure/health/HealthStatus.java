package org.corfudb.infrastructure.health;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class HealthStatus {
    
    @Getter
    private final Set<Issue> initHealthIssues;
    @Getter
    private final Set<Issue> runtimeHealthIssues;
    @Getter
    private final Component component;

    private InitStatus initStatus = InitStatus.UNKNOWN;

    public HealthStatus(Component component) {
        this.initHealthIssues = new LinkedHashSet<>();
        this.runtimeHealthIssues = new LinkedHashSet<>();
        this.component = component;
    }

    /**
     * Add init health issue -- health monitor now expects the component to be initialized.
     * If the component has not been previously initialized (UNKNOWN) or initialized (INITIALIZED),
     * adding init issue will move it to NOT_INITIALIZED, all runtime issues are cleared.
     * @param issue An init issue
     */
    public void addInitHealthIssue(Issue issue) {
        verifyComponent(issue);
        if (!issue.isInitIssue()) {
            log.warn("addInitHealthIssue: trying to add a non-init issue - {}", issue);
            throw new IllegalArgumentException("Only issues of type INIT are allowed");
        }
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
        verifyComponent(issue);
        if (initStatus == InitStatus.NOT_INITIALIZED) {
            initStatus = InitStatus.INITIALIZED;
        }
        initHealthIssues.remove(issue);
    }

    /**
     * Add runtime health issue -- health monitor now expects the component's runtime issue to recover.
     * If component has been not been previously initialized (INITIALIZED), adding the runtime issue will
     * result in exception. If the runtime issue is already present in the HealthStatus, this is a
     * NOOP and the previously added issue maintains it's position in the runtimeHealthIssues
     * LinkedHashSet.
     * @param issue An init issue
     */
    public void addRuntimeHealthIssue(Issue issue) {
        verifyComponent(issue);
        if (initStatus != InitStatus.INITIALIZED) {
            log.warn("addRuntimeHealthIssue: trying to add a runtime health issue to NOT_INITIALIZED component - {}", issue);
            throw new IllegalStateException("Runtime health issue can only be reported if the component is initialized");
        }
        runtimeHealthIssues.add(issue);
    }

    /**
     * Remove the runtime health issue -- report to health monitor that this issue is resolved.
     * @param issue An init issue
     */
    public void resolveRuntimeHealthIssue(Issue issue) {
        verifyComponent(issue);
        runtimeHealthIssues.remove(issue);
    }

    public boolean isInitHealthy() {
        return initHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    public boolean isRuntimeHealthy() {
        return runtimeHealthIssues.isEmpty() && initStatus == InitStatus.INITIALIZED;
    }

    private void verifyComponent(Issue issue) {
        if (issue.getComponent() != this.component) {
            log.warn("verifyComponent: this issue: {} is for the wrong component: {}", issue, this.component);
            String msg = String.format("Only the issues belonging to %s are allowed but got: %s", this.component,
                    issue.getComponent());

            throw new IllegalStateException(msg);
        }
    }
    /**
     * Get the last reported unique runtime issue (If the runtime issue is already present
     * in the HealthStatus and added again, it is not considered the latest since it's not resolved
     * yet).
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

    static enum InitStatus {
        UNKNOWN,
        NOT_INITIALIZED,
        INITIALIZED
    }
}
