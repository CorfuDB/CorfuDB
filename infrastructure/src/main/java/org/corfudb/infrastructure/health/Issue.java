package org.corfudb.infrastructure.health;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * An issue that's being reported to HealthMonitor
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class Issue {

    public static enum IssueId {
        INIT,
        SEQUENCER_REQUIRES_FULL_BOOTSTRAP,
        ORCHESTRATOR_TASK_FAILED,
        FAILURE_DETECTOR_TASK_FAILED,
        COMPACTION_CYCLE_FAILED,
        QUOTA_EXCEEDED_ERROR
    }

    /**
     * A component for which the issue is reported
     */
    @Getter
    private final Component component;
    /**
     * A unique issue id
     */
    @Getter
    private final IssueId issueId;
    /**
     * A readable description of the issue
     */
    @Getter
    @EqualsAndHashCode.Exclude
    private final String description;

    private static Issue issue(Component component, IssueId issueId, String description) {
        return new Issue(component, issueId, description);
    }

    /**
     * Create init issue for component. If reported, this signals to health monitor that the
     * component should soon be initialized.
     * @param A component
     * @return An init issue
     */
    public static Issue createInitIssue(Component component) {
        return Issue.issue(component, IssueId.INIT, component + " is not initialized");
    }

    /**
     * Create an init or health issue.
     * @param component A component
     * @param issueId A unique issueId
     * @param description A readable secription
     * @return An issue
     */
    public static Issue createIssue(Component component, IssueId issueId, String description) {
        return Issue.issue(component, issueId, description);
    }

    public boolean isInitIssue() {
        return issueId == IssueId.INIT;
    }

    public boolean isRuntimeIssue() {
        return !isInitIssue();
    }

}
