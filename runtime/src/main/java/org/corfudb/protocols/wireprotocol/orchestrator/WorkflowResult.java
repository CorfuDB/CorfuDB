package org.corfudb.protocols.wireprotocol.orchestrator;

/**
 * The return value of running a workflow.
 *
 * @author Maithem
 */
public enum WorkflowResult {
    /**
     * Workflow completed successfully
     */
    COMPLETED,

    /**
     * Workflow completed exceptionally
     */
    ERROR,

    /**
     * Orchestrator is not running the workflow
     */
    UNKNOWN,

    /**
     * Orchestrator is busy running another workflow
     * on the same endpoint
     */
    BUSY;

    public static byte[] getBytes(WorkflowResult val) {
        return val.name().getBytes();
    }
}
