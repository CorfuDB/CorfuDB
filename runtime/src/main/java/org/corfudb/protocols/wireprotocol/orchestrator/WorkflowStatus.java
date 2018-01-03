package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;

import javax.annotation.Nonnull;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponseType.WORKFLOW_STATUS;

/**
 *
 * The status that the orchestrator returns for corresponding orchestrator operations (i.e. query
 * orchestrator and run workflow)
 *
 * @author Maithem
 */
public class WorkflowStatus implements Response {

    @Getter
    final WorkflowResult result;

    /**
     * Create workflow status from a workflow result
     */
    public WorkflowStatus(@Nonnull WorkflowResult result) {
        this.result = result;
    }

    /**
     * Deserialize a byte array into a workflow status type.
     */
    public WorkflowStatus(@Nonnull byte[] bytes) {
        this.result = WorkflowResult.valueOf(new String(bytes));
    }

    @Override
    public OrchestratorResponseType getType() {
        return WORKFLOW_STATUS;
    }


    @Override
    public byte[] getSerialized() {
        return WorkflowResult.getBytes(result);
    }
}
