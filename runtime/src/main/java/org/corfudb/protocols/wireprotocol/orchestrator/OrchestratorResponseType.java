package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type of responses that can be sent from the Orchestrator Service.
 *
 * Created by Maithem on 1/2/18.
 */
@AllArgsConstructor
public enum OrchestratorResponseType {
    /**
     * The result of a workflow that executed
     */
    WORKFLOW_STATUS(0, WorkflowStatus::new);

    /**
     * Id of the response type
     */
    @Getter
    public final int type;

    /**
     * A function that map a serialized response into a
     * response object.
     */
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
