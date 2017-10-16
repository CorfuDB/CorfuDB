package org.corfudb.workflow;

public enum Status {
    // Workflow not yet started.
    INIT,
    // Workflow in progress.
    IN_PROGRESS,
    // Workflow completed without exceptions.
    COMPLETED,
    // Workflow aborted due to exceptions or cancelled.
    ABORTED,
    // Undefined status.
    UNDEFINED;
}
