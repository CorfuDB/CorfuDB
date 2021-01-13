package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 *
 * The type of responses that the orchestrator replies with.
 *
 * Created by Maithem on 1/2/18.
 */
@AllArgsConstructor
public enum OrchestratorResponseType {
    /**
     * The status of a workflow
     */
    WORKFLOW_STATUS(0),

    /**
     * Id of a created workflow
     */
    WORKFLOW_CREATED(1);

    @Getter
    public final int type;
}
