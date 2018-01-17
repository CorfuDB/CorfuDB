package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type of requests that can be made to the Orchestrator Service.
 *
 * @author Maithem
 */
@AllArgsConstructor
public enum OrchestratorRequestType {

    /**
     * Query a workflow id
     */
    QUERY(0, QueryRequest::new),

    /**
     * Add a new node to the cluster
     */
    ADD_NODE(1, AddNodeRequest::new);

    @Getter
    public final int type;

    @Getter
    final Function<byte[], Request> requestGenerator;

    /**
     * Map an int to an enum.
     */
    static final Map<Integer, OrchestratorRequestType> typeMap =
            Arrays.stream(OrchestratorRequestType.values())
                    .collect(Collectors.toMap(OrchestratorRequestType::getType, Function.identity()));
}
