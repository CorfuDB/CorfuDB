package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    WORKFLOW_STATUS(0, QueryResponse::new),

    /**
     * Id of a created workflow
     */
    WORKFLOW_CREATED(1, CreateWorkflowResponse::new);

    @Getter
    public final int type;

    @Getter
    final Function<byte[], Response> responseGenerator;

    /**
     * Map an int to an enum.
     */
    static final Map<Integer, OrchestratorResponseType> typeMap =
            Arrays.stream(OrchestratorResponseType.values())
                    .collect(Collectors.toMap(OrchestratorResponseType::getType,
                            java.util.function.Function.identity()));
}
