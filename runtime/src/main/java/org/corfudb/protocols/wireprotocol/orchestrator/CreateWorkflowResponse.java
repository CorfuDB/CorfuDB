package org.corfudb.protocols.wireprotocol.orchestrator;

import java.util.UUID;
import lombok.Getter;

import static org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponseType.WORKFLOW_CREATED;

/**
 * CreateWorkflowResponse returns the UUID of a created workflow.
 * @author Maithem
 */
public class CreateWorkflowResponse implements Response {

    @Getter
    public UUID workflowId;

    public CreateWorkflowResponse(UUID workflowId) {
        this.workflowId = workflowId;
    }

    @Override
    public OrchestratorResponseType getType() {
        return WORKFLOW_CREATED;
    }
}
